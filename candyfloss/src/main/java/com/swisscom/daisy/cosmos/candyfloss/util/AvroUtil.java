package com.swisscom.daisy.cosmos.candyfloss.util;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroUtil {
  public static GenericRecord jsonMapToGenericRecord(Map<String, Object> jsonMap, Schema schema) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      if (jsonMap.containsKey(field.name())) {
        avroRecord.put(field.name(), jsonMap.get(field.name()));
      }
    }
    return avroRecord;
  }
}
