package com.swisscom.daisy.cosmos.candyfloss.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swisscom.daisy.cosmos.candyfloss.config.PipelineConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.PipelineStepConfig;
import com.swisscom.daisy.cosmos.candyfloss.messages.AvroOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.FlattenedMessage;
import com.swisscom.daisy.cosmos.candyfloss.messages.JsonOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputMessage;
import com.swisscom.daisy.cosmos.candyfloss.util.AvroUtil;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializationProcessor
    implements Processor<String, FlattenedMessage, String, OutputMessage> {
  private final PipelineConfig pipelineConfig;
  private final String schemaRegistryUrl;
  private ProcessorContext<String, OutputMessage> context;
  private SchemaRegistryClient schemaRegistryClient;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger logger = LoggerFactory.getLogger(SerializationProcessor.class);
  private final Counter counterError =
      Counter.builder("streams_serialize_error")
          .description("Number of error messages that are discarded to dlq topic")
          .register(Metrics.globalRegistry);
  private final String dlqTopicName;
  private final String discardTopicName;

  public SerializationProcessor(
      PipelineConfig pipelineConfig,
      String schemaRegistryUrl,
      String dlqTopicName,
      String discardTopicName) {
    this.pipelineConfig = pipelineConfig;
    this.schemaRegistryUrl = schemaRegistryUrl;
    this.dlqTopicName = dlqTopicName;
    this.discardTopicName = discardTopicName;
  }

  @Override
  public void init(ProcessorContext<String, OutputMessage> context) {
    this.context = context;
    if (this.schemaRegistryUrl != null && !this.schemaRegistryUrl.isBlank()) {
      this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }
  }

  @Override
  public void process(Record<String, FlattenedMessage> record) {
    var key = record.key();
    var value = record.value();

    var kv = handleRecord(key, value);
    context.forward(new Record<>(kv.key, kv.value, record.timestamp()));
  }

  public KeyValue<String, OutputMessage> handleRecord(String key, FlattenedMessage value) {
    try {
      OutputMessage outputMessage;
      if (value.getTag() == null) {
        String jsonString = objectMapper.writeValueAsString(value.getValue().read("$"));
        outputMessage = new OutputMessage(this.discardTopicName, new JsonOutputValue(jsonString));
      } else {
        PipelineStepConfig stepConfig = pipelineConfig.getSteps().get(value.getTag());
        boolean isAvroRequested =
                Objects.equals(stepConfig.getOutputFormat(), Optional.of("AVRO"));
        boolean canProduceAvro = this.schemaRegistryClient != null;
        if (isAvroRequested) {
          if (canProduceAvro) {
            outputMessage = processAvro(stepConfig, value);
          } else {
            // FATAL ERROR condition: AVRO is requested but no schema registry URL was provided.
            counterError.increment();
            logger.error(
                "Routing to DLQ: AVRO output is configured for tag '{}', but schema.registry.url is not provided.",
                value.getTag());
            String errorMessage =
                String.format(
                    "{\"error\": \"Configuration Error: AVRO output for tag '%s' requires a schema.registry.url, which was not provided.\"}",
                    value.getTag());
            return KeyValue.pair(
                key, new OutputMessage(this.dlqTopicName, new JsonOutputValue(errorMessage)));
          }
        } else {
          // Default path for JSON
          outputMessage = processJson(stepConfig, value);
        }
      }
      return KeyValue.pair(key, outputMessage);
    } catch (Exception e) {
      counterError.increment();
      return KeyValue.pair(
          key,
          new OutputMessage(
              this.dlqTopicName,
              new JsonOutputValue(String.format("{\"error\": \"%s\"}", e.getMessage()))));
    }
  }

  private OutputMessage processAvro(PipelineStepConfig stepConfig, FlattenedMessage message) {
    try {
      Map<String, Object> jsonMap = message.getValue().read("$");
      String subject = stepConfig.getOutputSubject().toString();
      String schemaString = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
      Schema schema = new Schema.Parser().parse(schemaString);
      GenericRecord avroRecord = AvroUtil.jsonMapToGenericRecord(jsonMap, schema);
      return new OutputMessage(stepConfig.getOutputTopic(), new AvroOutputValue(avroRecord));
    } catch (Exception e) {
      counterError.increment();
      return new OutputMessage(
          this.dlqTopicName,
          new JsonOutputValue(String.format("{\"error\": \"%s\"}", e.getMessage())));
    }
  }

  private OutputMessage processJson(PipelineStepConfig stepConfig, FlattenedMessage message) {
    try {
      Map<String, Object> jsonMap = message.getValue().read("$");
      String jsonString = objectMapper.writeValueAsString(jsonMap);
      return new OutputMessage(stepConfig.getOutputTopic(), new JsonOutputValue(jsonString));
    } catch (Exception e) {
      counterError.increment();
      return new OutputMessage(
          this.dlqTopicName,
          new JsonOutputValue(String.format("{\"error\": \"%s\"}", e.getMessage())));
    }
  }
}
