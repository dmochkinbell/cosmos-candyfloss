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
  private final String outputTopic;
  private final Match match;
  private final List<Map<String, Object>> transform;
  private final Optional<NormalizeCountersConfig> normalizeCountersConfig;
  private final String outputFormat;
  private final String outputSubject;

  @SuppressWarnings("unchecked")
  public static PipelineStepConfig fromJson(
      String outputTopic,
      String outputFormat,
      String outputSubject,
      Map<String, Object> configs,
      String stepTag)
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
    if (outputFormat.equalsIgnoreCase("AVRO") && outputSubject.isEmpty()) {
      throw new InvalidConfigurations(
          "Pipeline step '"
              + stepTag
              + "' is configured for AVRO output but is missing 'output.avro.subject'.");
    }

    return new PipelineStepConfig(
        outputTopic, match, transform, normalizeCountersConfig, outputFormat, outputSubject);
  }
}
