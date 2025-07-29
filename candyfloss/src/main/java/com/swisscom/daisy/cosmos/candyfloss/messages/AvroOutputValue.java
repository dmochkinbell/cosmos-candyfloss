package com.swisscom.daisy.cosmos.candyfloss.messages;

import org.apache.avro.generic.GenericRecord;

/**
 * A record to hold a final Avro GenericRecord payload.
 * It implements the sealed OutputValue interface.
 * @param record The GenericRecord to be serialized and written to Kafka.
 */
public record AvroOutputValue(GenericRecord record) implements OutputValue {
}
