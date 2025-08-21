package com.swisscom.daisy.cosmos.candyfloss;

import com.swisscom.daisy.cosmos.candyfloss.messages.AvroOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.JsonOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputMessage;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputValue;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.vavr.NotImplementedError;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** A custom Serde for serializing the final OutputMessage to a Kafka topic. */
public class OutputSerde implements Serde<OutputMessage> {
  /**
   * Configures the Serde. This is called by Kafka Streams upon initialization. It's used here to
   * configure the internal KafkaAvroSerializer with the necessary schema.registry.url.
   *
   * @param configs The Kafka Streams configuration map.
   * @param isKey Whether this Serde is for a key or value.
   */
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {}

  @Override
  public Serializer<OutputMessage> serializer() {
    return new CustomSerializer(); // Pass the client
  }

  @Override
  public Deserializer<OutputMessage> deserializer() {
    // Deserialization is not needed for an output-only Serde.
    throw new NotImplementedError();
  }

  /**
   * The inner serializer class that contains the core logic for converting an OutputMessage into
   * bytes.
   */
  class CustomSerializer implements Serializer<OutputMessage> {
    private final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();

    public CustomSerializer() { // Add constructor
      avroSerializer.configure(
          Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://test"), false);
    }

    /**
     * Serializes the OutputMessage payload based on its concrete type. This method uses
     * `instanceof` checks for compatibility with all Java versions.
     *
     * @param topic The topic the record is being sent to.
     * @param data The OutputMessage to serialize.
     * @return A byte array representing the serialized data.
     */
    @Override
    public byte[] serialize(String topic, OutputMessage data) {
      if (data == null || data.getValue() == null) {
        return null;
      }

      final OutputValue value = data.getValue();
      if (value instanceof JsonOutputValue j) {
        return j.json().getBytes(StandardCharsets.UTF_8);
      } else if (value instanceof AvroOutputValue a) {
        return avroSerializer.serialize(topic, a.record());
      }

      // This case should be unreachable if all sealed types are handled.
      // It acts as a safeguard against future unhandled implementations.
      throw new IllegalArgumentException("Unknown OutputValue type: " + value.getClass().getName());
    }
  }
}
