package com.swisscom.daisy.cosmos.candyfloss.util;

import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class AvroUtil {

  public static GenericRecord jsonMapToGenericRecord(Map<String, Object> jsonMap, Schema schema) {
    GenericRecord avroRecord = new GenericData.Record(schema);
    for (Field field : schema.getFields()) {
      String fieldName = field.name();
      Schema fieldSchema = field.schema();
      Object value = jsonMap.get(fieldName);

      try {
        Object convertedValue = handleFieldValue(value, field, fieldSchema);
        avroRecord.put(fieldName, convertedValue);
      } catch (Exception e) {
        String errorMessage =
            String.format(
                "Error setting field '%s' (type %s): %s",
                fieldName, fieldSchema.getType(), e.getMessage());
        throw new RuntimeException(errorMessage, e); // Rethrow for Kafka Streams
      }
    }
    return avroRecord;
  }

  private static Object handleFieldValue(Object value, Field field, Schema fieldSchema) {
    if (value == null) {
      if (isNullable(fieldSchema)) {
        return null;
      } else if (field.defaultVal() != null) {
        return field.defaultVal();
      } else {
        throw new IllegalArgumentException(
            "Required field is missing and is not nullable and no default");
      }
    } else {
      return convertValue(value, fieldSchema);
    }
  }

  private static Object convertValue(Object value, Schema fieldSchema) {
    switch (fieldSchema.getType()) {
      case UNION:
        return handleUnion(value, fieldSchema);
      case RECORD:
        return jsonMapToGenericRecord((Map<String, Object>) value, fieldSchema);
      case STRING, ENUM:
        return value.toString();
      case INT:
        return convertToInt(value, fieldSchema);
      case LONG:
        return convertToLong(value, fieldSchema);
      case FLOAT:
        return convertToFloat(value, fieldSchema);
      case DOUBLE:
        return convertToDouble(value, fieldSchema);
      case BOOLEAN:
        return convertToBoolean(value, fieldSchema);
      case BYTES:
        return convertToBytes(value, fieldSchema);
      case ARRAY:
        throw new UnsupportedOperationException("ARRAY type conversion not implemented yet");
      case FIXED:
        throw new UnsupportedOperationException("FIXED type conversion not implemented yet");
      default:
        throw new IllegalArgumentException("Unsupported Avro type: " + fieldSchema.getType());
    }
  }

  private static Object handleUnion(Object value, Schema fieldSchema) {
    List<Schema> types = fieldSchema.getTypes();

    if (value == null && isNullable(fieldSchema)) {
      return null;
    }

    for (Schema type : types) {
      if (type.getType() != Schema.Type.NULL) {
        try {
          Object convertedValue = convertValue(value, type);
          return convertedValue;
        } catch (Exception e) {
        }
      }
    }
    if (isNullable(fieldSchema)) {
      return null;
    } else {
      throw new IllegalArgumentException(
          "Value '" + value + "' does not match any type in the union: " + fieldSchema);
    }
  }

  private static boolean isNullable(Schema fieldSchema) {
    return fieldSchema.getType() == Schema.Type.UNION
        && fieldSchema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
  }

  private static Integer convertToInt(Object value, Schema fieldSchema) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Cannot convert String to Integer: " + value, e);
      }
    }
    throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to Int");
  }

  private static Long convertToLong(Object value, Schema fieldSchema) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      try {
        return Long.parseLong((String) value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Cannot convert String to Long: " + value, e);
      }
    }
    throw new IllegalArgumentException("Cannot convert " + value.getClass().getName() + " to Long");
  }

  private static Float convertToFloat(Object value, Schema fieldSchema) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      try {
        return Float.parseFloat((String) value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Cannot convert String to Float: " + value, e);
      }
    }
    throw new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " to Float");
  }

  private static Double convertToDouble(Object value, Schema fieldSchema) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      try {
        return Double.parseDouble((String) value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Cannot convert String to Double: " + value, e);
      }
    }
    throw new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " to Double");
  }

  private static Boolean convertToBoolean(Object value, Schema fieldSchema) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " to Boolean");
  }

  private static byte[] convertToBytes(Object value, Schema fieldSchema) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) {
      return ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    throw new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " to byte[]");
  }
}
