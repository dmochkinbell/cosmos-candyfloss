package com.swisscom.daisy.cosmos.candyfloss;

import com.swisscom.daisy.cosmos.candyfloss.config.JsonKStreamApplicationConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.MetricRegistryConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.exceptions.InvalidConfigurations;
import com.swisscom.daisy.cosmos.candyfloss.messages.ValueErrorMessage;
import com.swisscom.daisy.cosmos.candyfloss.monitors.RestoreLogger;
import com.swisscom.daisy.cosmos.candyfloss.monitors.StateLogger;
import com.swisscom.daisy.cosmos.candyfloss.processors.*;
import com.swisscom.daisy.cosmos.candyfloss.transformations.Transformer;
import com.swisscom.daisy.cosmos.candyfloss.transformations.match.exceptions.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CandyflossKStreamsApplication {
  private static final Logger logger = LoggerFactory.getLogger(CandyflossKStreamsApplication.class);
  private final JsonKStreamApplicationConfig config;
  private final SchemaRegistryClient schemaRegistryClient; // Made final

  public CandyflossKStreamsApplication(
      JsonKStreamApplicationConfig config, SchemaRegistryClient schemaRegistryClient) {
    this.config = config;
    this.schemaRegistryClient = schemaRegistryClient;
  }

  public CandyflossKStreamsApplication(JsonKStreamApplicationConfig config) {
    this(config, null); // Default constructor with null SchemaRegistryClient
  }

  public static void main(String[] args)
      throws IOException, InvalidConfigurations, InvalidMatchConfiguration {
    MetricRegistryConfig prometheusRegistry = new MetricRegistryConfig();
    prometheusRegistry.bindJvm();

    logger.info("Loading configurations");
    Config conf = ConfigFactory.load();
    var appConf = JsonKStreamApplicationConfig.fromConfig(conf);

    SchemaRegistryClient schemaRegistryClient = null;
    String schemaRegistryUrl =
        appConf
            .getKafkaProperties()
            .getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    logger.info("Schema registry url loaded: {}", schemaRegistryUrl);
    if (schemaRegistryUrl != null && !schemaRegistryUrl.isEmpty()) {
      try {
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        logger.info("SchemaRegistryClient initialized with URL: {}", schemaRegistryUrl);
      } catch (Exception e) {
        logger.warn("Could not create SchemaRegistryClient. Avro output will be disabled.", e);
      }
    } else {
      logger.info("No schema registry URL provided. Avro output will be disabled.");
    }

    var app = new CandyflossKStreamsApplication(appConf, schemaRegistryClient);
    var topology = app.buildTopology();
    System.out.println(topology.describe());
    logger.info("Topology configured, now starting it");
    var kafkaStreams = new KafkaStreams(topology, appConf.getKafkaProperties());

    final CountDownLatch latch = new CountDownLatch(1);
    // attach shutdown handler to catch control-c
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                logger.info("Received shutdown signal, terminating Candyfloss");
                prometheusRegistry.close();
                kafkaStreams.close();
                latch.countDown();
              }
            });

    try {
      prometheusRegistry.start();

      StateLogger.setListener(kafkaStreams, new StateLogger(kafkaStreams));
      RestoreLogger.setListener(kafkaStreams, new RestoreLogger());

      kafkaStreams.start();
      latch.await();
      logger.info("Cosmos Candyfloss gracefully shutdown");
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  private String serializeAvroToJsonString(GenericRecord value) throws IOException {
    assert value.getSchema() != null;
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(value.getSchema());
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Encoder jsonEncoder = EncoderFactory.get().jsonEncoder(value.getSchema(), stream);
    writer.write(value, jsonEncoder);
    jsonEncoder.flush();
    return stream.toString();
  }

  private KStream<String, String> getAvroInput(StreamsBuilder builder) {
    String url =
        config
            .getKafkaProperties()
            .getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
    Map<String, String> serdeConfig =
        Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
    genericAvroSerde.configure(serdeConfig, false);

    KStream<String, GenericRecord> avroStream =
        builder.stream(
            config.getInputTopicName(), Consumed.with(Serdes.String(), genericAvroSerde));
    return avroStream.mapValues(
        value -> {
          try {
            return serializeAvroToJsonString(value);
          } catch (Exception ex) {
            logger.error("Error serializing Avro to JSON: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex); // Rethrow as RuntimeException for Kafka Streams
          }
        });
  }

  public Topology buildTopology() {
    Serde<String> stringSerde = Serdes.String();
    StreamsBuilder builder = new StreamsBuilder();

    // Persistent TimestampedStore to hold the counter values for the counter normalization step.
    // We are using timestamped, so we can clean up the old unused keys.
    var storeBuilder =
        Stores.timestampedKeyValueStoreBuilder(
            Stores.inMemoryKeyValueStore(config.getStateStoreName()),
            Serdes.Bytes(),
            Serdes.Bytes());
    builder.addStateStore(storeBuilder);

    KStream<String, String> inputStream;
    if (config.getInputType().equals(JsonKStreamApplicationConfig.InputType.JSON)) {
      inputStream =
          builder.stream(config.getInputTopicName(), Consumed.with(stringSerde, stringSerde));
    } else {
      inputStream = getAvroInput(builder);
    }

    var jsonStream =
        inputStream
            .process(FromJsonProcessor::new, Named.as("deserialize"))
            .split(Named.as("deserializeBranchError"))
            .branch(
                (k, v) -> v.isError(),
                Branched.withConsumer(
                    ks -> {
                      ks.to(
                          config.getDlqTopicName(),
                          Produced.with(stringSerde, new ValueErrorSerde<>()));
                      logger.warn("Message routed to DLQ due to input error");
                    }))
            .defaultBranch()
            .get("deserializeBranchError0")
            .mapValues(ValueErrorMessage::getValue);

    final Transformer preProcessor;
    if (config.getPreTransform().isEmpty()) {
      preProcessor = null;
    } else {
      preProcessor = new Transformer(config.getPreTransform());
    }

    var preTransformedStream =
        jsonStream
            .process(() -> new PreProcessor(preProcessor), Named.as("preTransformer"))
            .split(Named.as("preTransformerBranchError"))
            .branch(
                (k, v) -> v.isError(),
                Branched.withConsumer(
                    ks -> {
                      ks.to(
                          config.getDlqTopicName(),
                          Produced.with(stringSerde, new ValueErrorSerde<>()));
                      logger.warn("Message routed to DLQ due to pre-transform error");
                    }))
            .defaultBranch()
            .get("preTransformerBranchError0")
            .mapValues(ValueErrorMessage::getValue)
            // Pre-Transform can produce nulls if it's configured to skip some messages
            // We don't want to propagate those messages down in the pipeline
            .filter((k, v) -> v != null);

    var transformedStream =
        preTransformedStream
            .process(() -> new MessageProcessor(config.getPipeline()), Named.as("transform"))
            .split(Named.as("transformBranchError"))
            .branch(
                (k, v) -> v.isError(),
                Branched.withConsumer(
                    ks -> {
                      ks.to(
                          config.getDlqTopicName(),
                          Produced.with(stringSerde, new ValueErrorSerde<>()));
                      logger.warn("Message routed to DLQ due to transform error");
                    }))
            .defaultBranch()
            .get("transformBranchError0")
            .mapValues(ValueErrorMessage::getValue);

    var flattenStream =
        transformedStream
            .process(FlattenProcessor::new, Named.as("flatten"))
            .split(Named.as("flattenBranchError"))
            .branch(
                (k, v) -> v.isError(),
                Branched.withConsumer(
                    ks -> {
                      ks.to(
                          config.getDlqTopicName(),
                          Produced.with(stringSerde, new ValueErrorSerde<>()));
                      logger.warn("Message routed to DLQ due to flatten error");
                    }))
            .defaultBranch()
            .get("flattenBranchError0")
            .mapValues(ValueErrorMessage::getValue);

    var counterNormalizedStream =
        flattenStream
            .process(
                () ->
                    new CounterNormalizationProcessor(
                        config.getPipeline(),
                        config.getStateStoreName(),
                        config.getMaxCounterCacheAge(),
                        config.getIntCounterWrapAroundLimit(),
                        config.getLongCounterWrapAroundLimit(),
                        config.getCounterWrapAroundTimeMs(),
                        Duration.ofMillis(config.getMaxCounterCacheAge()),
                        config.getOldCountersScanFrequency()),
                Named.as("counterNormalization"),
                config.getStateStoreName())
            .split(Named.as("counterNormalizationBranchError"))
            .branch(
                (k, v) -> v.isError(),
                Branched.withConsumer(
                    ks -> {
                      ks.to(
                          config.getDlqTopicName(),
                          Produced.with(stringSerde, new ValueErrorSerde<>()));
                      logger.warn("Message routed to DLQ due to counter normalization error");
                    }))
            .defaultBranch()
            .get("counterNormalizationBranchError0")
            .mapValues(ValueErrorMessage::getValue);

    var stringStream =
        counterNormalizedStream.process(
            () ->
                new SerializationProcessor(
                    config.getPipeline(),
                    config.getDlqTopicName(),
                    config.getDiscardTopicName(),
                    schemaRegistryClient),
            Named.as("serialize"));

    stringStream.to(
        (key, value, recordContext) -> value.getOutputTopic(),
        Produced.with(
            stringSerde,
            new OutputSerde(
                config
                    .getKafkaProperties()
                    .getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG))));
    return builder.build();
  }
}
