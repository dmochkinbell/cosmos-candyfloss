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
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
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
    private TestInputTopic<String, String> inputTopic;
    private Map<String, TestOutputTopic<String, GenericRecord>> outputTopics;
    private SchemaRegistryClient schemaRegistryClient;
    private Schema outputSchema;
    private String outputTopicName;

    @BeforeEach
    public void setup()
            throws IOException, InvalidConfigurations, InvalidMatchConfiguration,
            RestClientException {
        setupTopology("avro-output/application.conf");
    }

    @AfterEach
    public void cleanUp() {
        topologyTestDriver.close();
    }

    private void setupTopology(String applicationConfPath)
            throws IOException, InvalidConfigurations, InvalidMatchConfiguration,
            RestClientException {
        Config config = ConfigFactory.load(applicationConfPath);
        appConf = JsonKStreamApplicationConfig.fromConfig(config);
        var schemaUrl =
                appConf
                        .getKafkaProperties()
                        .getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        var scope = MockSchemaRegistry.validateAndMaybeGetMockScope(List.of(schemaUrl));
        schemaRegistryClient = MockSchemaRegistry.getClientForScope(scope);
        CandyflossKStreamsApplication app = new CandyflossKStreamsApplication(appConf);
        final Topology topology = app.buildTopology();
        topologyTestDriver = new TopologyTestDriver(topology, appConf.getKafkaProperties());
        var stringSerde = Serdes.String();
        inputTopic =
                topologyTestDriver.createInputTopic(
                        appConf.getInputTopicName(), stringSerde.serializer(), stringSerde.serializer());

        // Determine the output topic name and schema
        Optional<Tuple2<String, Schema>> avroOutputConfig =
                appConf.getPipeline().getSteps().values().stream()
                        .filter(
                                x ->
                                        x.getOutputFormat()
                                                == com.swisscom.daisy.cosmos.candyfloss.config.PipelineStepConfig
                                                .OutputFormat.AVRO)
                        .findFirst()
                        .map(
                                x -> {
                                    outputTopicName = x.getOutputTopic();
                                    try {
                                        String subject = x.getAvroSubjectName().get();
                                        String schemaString =
                                                schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
                                        outputSchema = new Schema.Parser().parse(schemaString);
                                        return Tuple.of(x.getOutputTopic(), outputSchema);
                                    } catch (Exception e) {
                                        throw new RuntimeException(
                                                "Error getting schema for output topic: " + x.getOutputTopic(), e);
                                    }
                                });

        if (avroOutputConfig.isPresent()) {
            Tuple2<String, Schema> configTuple = avroOutputConfig.get();
            outputTopics =
                    Collections.singletonMap(
                            configTuple._1,
                            topologyTestDriver.createOutputTopic(
                                    configTuple._1,
                                    stringSerde.deserializer(),
                                    Serdes.serdeFrom(stringSerde.serializer(), new AvroGenericRecordDeserializer(schemaRegistryClient)).deserializer())); // Use Avro deserializer
        } else {
            outputTopics = Collections.emptyMap();
        }

        outputTopics.put(
                appConf.getDlqTopicName(),
                topologyTestDriver.createOutputTopic(
                        appConf.getDlqTopicName(), stringSerde.deserializer(), stringSerde.deserializer()));
        outputTopics.put(
                appConf.getDiscardTopicName(),
                topologyTestDriver.createOutputTopic(
                        appConf.getDiscardTopicName(), stringSerde.deserializer(), stringSerde.deserializer()));
    }

    @Test
    public void testAvroOutput() throws IOException, RestClientException, InvalidMatchConfiguration, InvalidConfigurations {
        setupTopology("avro-input/application.conf");
        if (outputTopics.isEmpty()) {
            logger.warn("No Avro output configured in the pipeline. Skipping Avro output test.");
            return;
        }
        var outputTopic = outputTopics.get(outputTopicName);

        // 1. Register input and output schema
        var parser = new Schema.Parser();
        var inputSchema =
                parser.parse(
                        getClass().getClassLoader().getResourceAsStream("avro-output/test-input-schema.avsc"));
        schemaRegistryClient.register(appConf.getInputTopicName() + "-value", inputSchema);

        // 2. Send input message
        GenericRecord avroInputMsg = genDeserializeFromJson("{\"event\":\"value\"}", schema);

        inputTopic.pipeInput(avroInputMsg);

        // 3. Read the Avro output
        List<GenericRecord> actualOutput = outputTopic.readValuesToList();

        // 4. Assert the output
        assertFalse(actualOutput.isEmpty(), "Output should not be empty");
        GenericRecord actualRecord = actualOutput.get(0);

        // Create expected Avro record
        GenericRecord expectedRecord = new GenericData.Record(outputSchema);
        expectedRecord.put("transformed_event", "value");

        assertEquals(expectedRecord, actualRecord, "Avro output does not match expected value");

        // 5. Verify DLQ is empty
        List<String> dlqOutput = outputTopics.get(appConf.getDlqTopicName()).readValuesToList();
        assertTrue(dlqOutput.isEmpty(), "DLQ should be empty in this scenario");
    }

    // Custom Deserializer for Avro GenericRecord
    private static class AvroGenericRecordDeserializer
            implements org.apache.kafka.common.serialization.Deserializer<GenericRecord> {

        private final SchemaRegistryClient schemaRegistryClient;

        public AvroGenericRecordDeserializer(SchemaRegistryClient schemaRegistryClient) {
            this.schemaRegistryClient = schemaRegistryClient;
        }

        @Override
        public GenericRecord deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                // Extract schema from Schema Registry based on topic and version
                String schemaString = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema();
                Schema schema = new Schema.Parser().parse(schemaString);

                // Create a DatumReader to read the Avro data
                org.apache.avro.io.DatumReader<GenericRecord> datumReader =
                        new org.apache.avro.io.generic.GenericDatumReader<>(schema);

                // Create a BinaryDecoder to decode the data
                org.apache.avro.io.BinaryDecoder binaryDecoder =
                        org.apache.avro.io.DecoderFactory.get().binaryDecoder(data, null);

                // Deserialize the data into a GenericRecord
                return datumReader.read(null, binaryDecoder);

            } catch (Exception e) {
                throw new RuntimeException("Error deserializing Avro data", e);
            }
        }
    }
}