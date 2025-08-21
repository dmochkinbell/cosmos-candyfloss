package com.swisscom.daisy.cosmos.candyfloss;

import static org.junit.jupiter.api.Assertions.*;

import com.swisscom.daisy.cosmos.candyfloss.config.JsonKStreamApplicationConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.exceptions.InvalidConfigurations;
import com.swisscom.daisy.cosmos.candyfloss.transformations.match.exceptions.InvalidMatchConfiguration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroOutputTest {
  private static final Logger logger = LoggerFactory.getLogger(AvroOutputTest.class);
  private TopologyTestDriver topologyTestDriver;
  private JsonKStreamApplicationConfig appConf;
  private TestInputTopic<String, GenericRecord> inputTopic;
  private Map<String, TestOutputTopic<String, GenericRecord>> avroOutputTopics =
      new HashMap<>(); // Avro-specific map
  private Map<String, TestOutputTopic<String, String>> jsonOutputTopics =
      new HashMap<>(); // JSON-specific map
  private SchemaRegistryClient schemaRegistryClient;
  private Schema outputSchema;
  private String schemaRegistryUrl;

  @BeforeEach
  public void setup()
      throws IOException, InvalidConfigurations, InvalidMatchConfiguration, RestClientException {
    setupTopology("avro-output/application.conf");

    var parser = new Schema.Parser();
    var schema =
        parser.parse(
            getClass().getClassLoader().getResourceAsStream("avro-output/test-schema.avsc"));

    var outputSchemaParser = new Schema.Parser();
    outputSchema =
        outputSchemaParser.parse(
            getClass().getClassLoader().getResourceAsStream("avro-output/test-output-schema.avsc"));

    schemaRegistryUrl =
        appConf
            .getKafkaProperties()
            .getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);

    String scope = MockSchemaRegistry.validateAndMaybeGetMockScope(List.of(schemaRegistryUrl));
    schemaRegistryClient = MockSchemaRegistry.getClientForScope(scope);

    try {
      schemaRegistryClient.register(appConf.getInputTopicName() + "-value", schema);
      logger.info("Registered input schema for topic: {}", appConf.getInputTopicName());
    } catch (Exception e) {
      logger.error("Error registering input schema:", e);
      throw e;
    }

    try {
      schemaRegistryClient.register("test-subject_value", outputSchema);
      logger.info("Registered output schema for topic: output1");
    } catch (Exception e) {
      logger.error("Error registering output schema:", e);
      throw e;
    }
  }

  @AfterEach
  public void cleanUp() {
    topologyTestDriver.close();
  }

  @SuppressWarnings("unchecked")
  private <T> Serializer<T> getSerializer(boolean isKey) {
    Map<String, Object> map = new HashMap<>();
    map.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    map.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false); // Do not auto-register

    Serializer<T> serializer = (Serializer<T>) new KafkaAvroSerializer(schemaRegistryClient);
    serializer.configure(map, isKey);

    return serializer;
  }

  @SuppressWarnings("unchecked")
  private <T> Deserializer<T> getAvroOutputDeserializer(boolean isKey) {
    Map<String, Object> map = new HashMap<>();
    map.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    map.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);

    Deserializer<T> deserializer =
        (Deserializer<T>) new KafkaAvroDeserializer(schemaRegistryClient);
    deserializer.configure(map, isKey);

    return deserializer;
  }

  public GenericRecord genDeserializeFromJson(String message, Schema schema) throws IOException {
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, message);
    return reader.read(null, decoder);
  }

  private void setupTopology(String applicationConfPath)
      throws IOException, InvalidConfigurations, InvalidMatchConfiguration {
    Config config = ConfigFactory.load(applicationConfPath);
    appConf = JsonKStreamApplicationConfig.fromConfig(config);
    schemaRegistryUrl =
        appConf
            .getKafkaProperties()
            .getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    var scope = MockSchemaRegistry.validateAndMaybeGetMockScope(List.of(schemaRegistryUrl));
    schemaRegistryClient = MockSchemaRegistry.getClientForScope(scope);
    CandyflossKStreamsApplication app =
        new CandyflossKStreamsApplication(appConf, schemaRegistryClient);

    final Topology topology = app.buildTopology();
    topologyTestDriver = new TopologyTestDriver(topology, appConf.getKafkaProperties());
    var stringSerde = Serdes.String();
    Serializer<GenericRecord> avroSerializer = getSerializer(false);
    inputTopic =
        topologyTestDriver.createInputTopic(
            appConf.getInputTopicName(), stringSerde.serializer(), avroSerializer);

    // Populate Avro and JSON output topic maps
    appConf
        .getPipeline()
        .getSteps()
        .values()
        .forEach(
            x -> {
              String outputTopicName = x.getOutputTopic();
              if (x.getOutputFormat().equalsIgnoreCase("AVRO")) {
                Deserializer<GenericRecord> avroDeserializer = getAvroOutputDeserializer(false);
                avroOutputTopics.put(
                    outputTopicName,
                    topologyTestDriver.createOutputTopic(
                        outputTopicName, stringSerde.deserializer(), avroDeserializer));
              } else {
                jsonOutputTopics.put(
                    outputTopicName,
                    topologyTestDriver.createOutputTopic(
                        outputTopicName, stringSerde.deserializer(), stringSerde.deserializer()));
              }
            });

    // Create DLQ and Discard topics with String deserializer
    jsonOutputTopics.put(
        appConf.getDlqTopicName(),
        topologyTestDriver.createOutputTopic(
            appConf.getDlqTopicName(), stringSerde.deserializer(), stringSerde.deserializer()));
    jsonOutputTopics.put(
        appConf.getDiscardTopicName(),
        topologyTestDriver.createOutputTopic(
            appConf.getDiscardTopicName(), stringSerde.deserializer(), stringSerde.deserializer()));
  }

  @Test
  public void testAvroApplication()
      throws IOException, InvalidConfigurations, InvalidMatchConfiguration, RestClientException {
    TestOutputTopic<String, GenericRecord> outputTopic =
        avroOutputTopics.get(appConf.getPipeline().getSteps().get("output1").getOutputTopic());

    var parser = new Schema.Parser();
    var schema =
        parser.parse(
            getClass().getClassLoader().getResourceAsStream("avro-output/test-schema.avsc"));

    GenericRecord avroInputMsg = genDeserializeFromJson("{\"event\":\"value\"}", schema);
    inputTopic.pipeInput("key", avroInputMsg); // Provide a key

    List<GenericRecord> actualOutput = outputTopic.readValuesToList();

    List<String> dlqOutput = jsonOutputTopics.get(appConf.getDlqTopicName()).readValuesToList();
    if (!dlqOutput.isEmpty()) {
      logger.error("Messages in DLQ: {}", dlqOutput);
      fail("Messages found in DLQ. Check the logs for errors.");
    }

    List<String> discardOutput =
        jsonOutputTopics.get(appConf.getDiscardTopicName()).readValuesToList();
    if (!discardOutput.isEmpty()) {
      logger.error("Messages in Discard Topic: {}", discardOutput);
      fail("Messages found in Discard Topic. Check the logs for errors.");
    }

    GenericRecord expectedRecord = new GenericData.Record(outputSchema);
    expectedRecord.put("transformed_event", "value");

    assertEquals(1, actualOutput.size());
    GenericRecord actualRecord = actualOutput.get(0);
    assertEquals(
        expectedRecord.get("transformed_event").toString(),
        actualRecord.get("transformed_event").toString());
  }
}
