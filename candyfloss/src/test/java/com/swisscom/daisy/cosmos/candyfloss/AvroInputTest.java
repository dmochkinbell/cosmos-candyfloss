package com.swisscom.daisy.cosmos.candyfloss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.swisscom.daisy.cosmos.candyfloss.config.JsonKStreamApplicationConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.PipelineStepConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.exceptions.InvalidConfigurations;
import com.swisscom.daisy.cosmos.candyfloss.messages.JsonOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputMessage;
import com.swisscom.daisy.cosmos.candyfloss.transformations.match.exceptions.InvalidMatchConfiguration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

public class AvroInputTest {
  private TopologyTestDriver topologyTestDriver;
  private JsonKStreamApplicationConfig appConf;
  private TestInputTopic<String, GenericRecord> inputTopic;
  private Map<String, TestOutputTopic<String, byte[]>> outputTopics; // Changed to byte[]
  private SchemaRegistryClient schemaRegistryClient;
  private Schema schema;

  @BeforeEach
  public void setup()
      throws IOException, InvalidConfigurations, InvalidMatchConfiguration,
          RestClientException {
    setupTopology("avro-input/application.conf");
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
    schemaRegistryClient = new MockSchemaRegistryClient(); // Mock for testing
    CandyflossKStreamsApplication app = new CandyflossKStreamsApplication(appConf);
    final Topology topology = app.buildTopology();
    topologyTestDriver = new TopologyTestDriver(topology, appConf.getKafkaProperties());
    var stringSerde = Serdes.String();

    // --- Register the schema in the mock registry ---
    var parser = new Schema.Parser();
    schema =
        parser.parse(
            getClass().getClassLoader().getResourceAsStream("avro-input/test-schema.avsc"));

    String subjectName = appConf.getInputTopicName() + "-value";
    try {
      // Check if the schema is already registered
      SchemaMetadata existingSchema = schemaRegistryClient.getLatestSchemaMetadata(subjectName);
      if (!existingSchema.getSchema().equals(schema.toString())) {
        // If the schema has changed, register the new schema
        schemaRegistryClient.register(subjectName, schema);
      }
    } catch (IOException | RestClientException e) {
      // If the schema is not found, register the new schema
      schemaRegistryClient.register(subjectName, schema);
    }

    // --- Configure the Avro Serializer ---
    final Map<String, String> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
    final GenericAvroSerde avroSerde = new GenericAvroSerde(schemaRegistryClient);
    avroSerde.configure(serdeConfig, false); // 'false' for value serde

    inputTopic =
        topologyTestDriver.createInputTopic(
            appConf.getInputTopicName(), stringSerde.serializer(), avroSerde.serializer());

    // Create output topics
    outputTopics =
        appConf.getPipeline().getSteps().values().stream()
            .collect(
                Collectors.toMap(
                    PipelineStepConfig::getOutputTopic,
                    x ->
                        topologyTestDriver.createOutputTopic(
                            x.getOutputTopic(),
                            stringSerde.deserializer(),
                            Serdes.ByteArray().deserializer())));
  }

  @Test
  public void testApplication() throws IOException, InvalidConfigurations, InvalidMatchConfiguration {
    // --- Create Avro record ---
    GenericRecord avroInputMsg = new GenericData.Record(schema);
    avroInputMsg.put("event", "value");

    // --- Send to input topic ---
    inputTopic.pipeInput("key", avroInputMsg);

    // --- Get the output topic ---
    var outputTopic =
        outputTopics.get(appConf.getPipeline().getSteps().get("output1").getOutputTopic());

    // --- Read the output and assert ---
    List<byte[]> rawOutputMessages = outputTopic.readValuesToList(); // Read as byte[]
    assertEquals(1, rawOutputMessages.size());

    // --- Manually Deserialize using OutputSerde ---
    OutputSerde outputSerde = new OutputSerde();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, appConf.getKafkaProperties().getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
    outputSerde.configure(config, false);

    OutputMessage outputMessage = outputSerde.deserializer().deserialize(null, rawOutputMessages.get(0));

    assertTrue(outputMessage.getValue() instanceof JsonOutputValue); // Now it's a JsonOutputValue
    JsonOutputValue jsonOutputValue = (JsonOutputValue) outputMessage.getValue(); // Cast
    assertEquals("{\"transformed_event\":\"value\"}", jsonOutputValue.json());
  }
}