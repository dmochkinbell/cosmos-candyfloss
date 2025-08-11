package com.swisscom.daisy.cosmos.candyfloss.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.swisscom.daisy.cosmos.candyfloss.config.PipelineConfig;
import com.swisscom.daisy.cosmos.candyfloss.config.PipelineStepConfig;
import com.swisscom.daisy.cosmos.candyfloss.messages.AvroOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.JsonOutputValue;
import com.swisscom.daisy.cosmos.candyfloss.messages.OutputMessage;
import com.swisscom.daisy.cosmos.candyfloss.util.AvroUtil;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class SerializationProcessor
    implements Processor<String, DocumentContext, String, OutputMessage> {
  private final PipelineConfig pipelineConfig;
  private final String schemaRegistryUrl;
  private ProcessorContext<String, OutputMessage> context;
  private SchemaRegistryClient schemaRegistryClient;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public SerializationProcessor(PipelineConfig pipelineConfig, String schemaRegistryUrl) {
    this.pipelineConfig = pipelineConfig;
    this.schemaRegistryUrl = schemaRegistryUrl;
  }

  @Override
  public void init(ProcessorContext<String, OutputMessage> context) {
    this.context = context;
    this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
  }

  @Override
  public void process(Record<String, DocumentContext> record) {
    String pipelineTag = record.value().read("$._pipeline_tag");
    PipelineStepConfig stepConfig = pipelineConfig.getSteps().get(pipelineTag);

    if (stepConfig == null) return;

    Map<String, Object> jsonMap = record.value().json();
    OutputMessage outputMessage;

    if (stepConfig.getOutputFormat() == PipelineStepConfig.OutputFormat.AVRO) {
      try {
        String subject = stepConfig.getAvroSubjectName().get();
        String schemaString = schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema();
        Schema schema = new Schema.Parser().parse(schemaString);
        GenericRecord avroRecord = AvroUtil.jsonMapToGenericRecord(jsonMap, schema);

        outputMessage =
            new OutputMessage(stepConfig.getOutputTopic(), new AvroOutputValue(avroRecord));
      } catch (IOException | RestClientException e) {
        // Handle failure to fetch schema or build record
        e.printStackTrace();
        return;
      }
    } else { // Default to JSON
      try {
        String jsonString = objectMapper.writeValueAsString(jsonMap);
        outputMessage =
            new OutputMessage(stepConfig.getOutputTopic(), new JsonOutputValue(jsonString));
      } catch (JsonProcessingException e) {
        e.printStackTrace();
        return;
      }
    }
    context.forward(record.withValue(outputMessage));
  }
}
