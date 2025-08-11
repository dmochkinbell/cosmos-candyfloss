package com.swisscom.daisy.cosmos.candyfloss.messages;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Last step for outputting a message. It holds the destination topic and a type-safe OutputValue
 * payload.
 */
@Data
@AllArgsConstructor
public class OutputMessage {
  private final String outputTopic;
  private final OutputValue value;
}
