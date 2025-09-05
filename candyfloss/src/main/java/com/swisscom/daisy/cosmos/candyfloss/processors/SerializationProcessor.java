package com.swisscom.daisy.cosmos.candyfloss.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.swisscom.daisy.cosmos.candyfloss.config.PipelineConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.PipelineStepConfig;
import com.swisscom.daisy.cosmos.candyfloss.messages.AvroOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.FlattenedMessage;
import com.swisscom.daisy.cosmos.candyfloss.messages.JsonOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputMessage;
import com.swisscom.daisy.cosmos.candyfloss.util.AvroUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.util.Map;
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
  private final SchemaRegistryClient schemaRegistryClient;
  private ProcessorContext<String, OutputMessage> context;
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
      String dlqTopicName,
      String discardTopicName,
      SchemaRegistryClient schemaRegistryClient) {
    this.pipelineConfig = pipelineConfig;
    this.dlqTopicName = dlqTopicName;
    this.discardTopicName = discardTopicName;
    this.schemaRegistryClient = schemaRegistryClient;
  }

  @Override
  public void init(ProcessorContext<String, OutputMessage> context) {
    this.context = context;
  }

  @Override
  public void process(Record<String, FlattenedMessage> record) {
    var key = record.key();
    var flattenedMessage = record.value();

    var kv = handleRecord(key, flattenedMessage);
    context.forward(new Record<>(kv.key, kv.value, record.timestamp()));
  }

  public KeyValue<String, OutputMessage> handleRecord(String key, FlattenedMessage value) {
    try {
      OutputMessage outputMessage;
      if (value.getTag() == null) {
        logger.warn(
            "Message with key {} has a null tag, indicating it was not transformed. Routing to discard topic '{}'.",
            key,
            this.discardTopicName);

        String originalData = objectMapper.writeValueAsString(value.getValue().read("$"));
        outputMessage = new OutputMessage(this.discardTopicName, new JsonOutputValue(originalData));

      } else {
        PipelineStepConfig stepConfig = pipelineConfig.getSteps().get(value.getTag());
        boolean isAvroRequested = stepConfig.getOutputFormat().equalsIgnoreCase("AVRO");
        boolean canProduceAvro = this.schemaRegistryClient != null;

        if (isAvroRequested) {
          if (canProduceAvro) {
            outputMessage = processAvro(stepConfig, value);
          } else {
            counterError.increment();
            logger.error(
                "Routing to DLQ: AVRO output is configured for tag '{}', but schema.registry.url is not provided.",
                value.getTag());
            String errorMessage =
                String.format(
                    "{\"error\": \"Configuration Error: AVRO output for tag '%s' requires a schema.registry.url, which was not provided.\"}",
                    value.getTag());
            outputMessage = new OutputMessage(this.dlqTopicName, new JsonOutputValue(errorMessage));
          }
        } else {
          // Default path for JSON
          outputMessage = processJson(stepConfig, value);
        }
      }

      outputMessage.setTag(value.getTag());
      return KeyValue.pair(key, outputMessage);

    } catch (Exception e) {
      counterError.increment();
      logger.error(
          "Critical error in SerializationProcessor for key {}: {}", key, e.getMessage(), e);
      OutputMessage errorMessage =
          new OutputMessage(
              this.dlqTopicName,
              new JsonOutputValue(String.format("{\"error\": \"%s\"}", e.getMessage())));
      errorMessage.setTag(null);
      return KeyValue.pair(key, errorMessage);
    }
  }

  private OutputMessage processAvro(PipelineStepConfig stepConfig, FlattenedMessage message)
      throws IOException, RestClientException {
    Map<String, Object> jsonMap = message.getValue().read("$");
    String subject = stepConfig.getOutputSubject();
    String schemaString = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
    Schema schema = new Schema.Parser().parse(schemaString);
    GenericRecord avroRecord = AvroUtil.jsonMapToGenericRecord(jsonMap, schema);
    return new OutputMessage(stepConfig.getOutputTopic(), new AvroOutputValue(avroRecord));
  }

  private OutputMessage processJson(PipelineStepConfig stepConfig, FlattenedMessage message)
      throws IOException {
    Map<String, Object> jsonMap = message.getValue().read("$");
    String jsonString = objectMapper.writeValueAsString(jsonMap);
    return new OutputMessage(stepConfig.getOutputTopic(), new JsonOutputValue(jsonString));
  }
}
