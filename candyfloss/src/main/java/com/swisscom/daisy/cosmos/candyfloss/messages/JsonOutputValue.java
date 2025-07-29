package com.swisscom.daisy.cosmos.candyfloss.messages;

/**
 * A record to hold a final JSON string payload.
 * It implements the sealed OutputValue interface.
 * @param json The JSON string to be written to Kafka.
 */
public record JsonOutputValue(String json) implements OutputValue {
}
