package com.swisscom.daisy.cosmos.candyfloss.messages;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OutputMessage {
  private String outputTopic;
  private OutputValue value;
  private String tag;

  // A constructor for cases where the tag is not immediately available.
  public OutputMessage(String outputTopic, OutputValue value) {
    this.outputTopic = outputTopic;
    this.value = value;
    this.tag = null; // Default to null
  }
}
