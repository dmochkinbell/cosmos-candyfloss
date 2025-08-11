package com.swisscom.daisy.cosmos.candyfloss.config;

import com.swisscom.daisy.cosmos.candyfloss.config.exceptions.InvalidConfigurations;
import com.swisscom.daisy.cosmos.candyfloss.transformations.match.Match;
import com.swisscom.daisy.cosmos.candyfloss.transformations.match.MatchBuilder;
import com.swisscom.daisy.cosmos.candyfloss.transformations.match.exceptions.InvalidMatchConfiguration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PipelineStepConfig {
  public enum OutputFormat {
    JSON,
    AVRO
  }

  private final String outputTopic;
  private final Match match;
  private final List<Map<String, Object>> transform;
  private final Optional<NormalizeCountersConfig> normalizeCountersConfig;
  private final OutputFormat outputFormat;
  private final Optional<String> avroSubjectName;

  @SuppressWarnings("unchecked")
  public static PipelineStepConfig fromJson(
      String outputTopic, Map<String, Object> configs, String stepTag)
      throws InvalidConfigurations, InvalidMatchConfiguration {
    var match = MatchBuilder.fromJson((Map<String, Object>) configs.get("match"), stepTag);
    var transform = (List<Map<String, Object>>) configs.get("transform");
    final Optional<NormalizeCountersConfig> normalizeCountersConfig;
    if (configs.containsKey("normalizeCounters")) {
      normalizeCountersConfig =
          Optional.of(
              NormalizeCountersConfig.fromJson(
                  (Map<String, Object>) configs.get("normalizeCounters"), stepTag));
    } else {
      normalizeCountersConfig = Optional.empty();
    }

    final Map<String, Object> outputConfig = (Map<String, Object>) configs.get("output");
    final OutputFormat outputFormat;
    final Optional<String> avroSubjectName;

    if (outputConfig != null) {
      outputFormat =
          OutputFormat.valueOf(
              ((String) outputConfig.getOrDefault("format", "JSON")).toUpperCase());
      avroSubjectName = Optional.ofNullable((String) outputConfig.get("avroSubjectName"));
    } else {
      // Defaults if "output" block is missing
      outputFormat = OutputFormat.JSON;
      avroSubjectName = Optional.empty();
    }

    if (outputFormat == OutputFormat.AVRO && avroSubjectName.isEmpty()) {
      throw new InvalidConfigurations(
          "Pipeline step '"
              + stepTag
              + "' is configured for AVRO output but is missing 'avroSubjectName'.");
    }

    return new PipelineStepConfig(
        outputTopic, match, transform, normalizeCountersConfig, outputFormat, avroSubjectName);
  }
}
