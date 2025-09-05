package com.swisscom.daisy.cosmos.candyfloss;

import com.swisscom.daisy.cosmos.candyfloss.messages.AvroOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.JsonOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputMessage;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputValue;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.vavr.NotImplementedError;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class OutputSerde implements Serde<OutputMessage> {

  private final String schemaRegistryUrl;

  public OutputSerde(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  public Serializer<OutputMessage> serializer() {
    return new CustomSerializer(schemaRegistryUrl);
  }

  @Override
  public Deserializer<OutputMessage> deserializer() {
    throw new NotImplementedError();
  }

  static class CustomSerializer implements Serializer<OutputMessage> {
    private final String schemaRegistryUrl;

    public CustomSerializer(String schemaRegistryUrl) {
      this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(String topic, OutputMessage data) {
      if (data == null || data.getValue() == null) {
        return null;
      }

      final OutputValue value = data.getValue();
      if (value instanceof JsonOutputValue j) {
        return j.json().getBytes(StandardCharsets.UTF_8);
      } else if (value instanceof AvroOutputValue a) {
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer();
        if (schemaRegistryUrl != null && !schemaRegistryUrl.isEmpty()) {
          avroSerializer.configure(
              java.util.Map.of(
                  io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
                      .SCHEMA_REGISTRY_URL_CONFIG,
                  schemaRegistryUrl),
              false);
        } else {
          throw new IllegalArgumentException(
              "Schema Registry URL must be provided for avro messages");
        }
        return avroSerializer.serialize(topic, a.record());
      }

      throw new IllegalArgumentException("Unknown OutputValue type: " + value.getClass().getName());
    }
  }
}
