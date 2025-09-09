package com.swisscom.daisy.cosmos.candyfloss.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
        throw new RuntimeException(errorMessage, e);
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
            "Required field '" + field.name() + "' is missing and is not nullable and no default");
      }
    } else {
      return convertValue(value, fieldSchema);
    }
  }

  private static Object convertValue(Object value, Schema fieldSchema) {
    try {
      switch (fieldSchema.getType()) {
        case UNION:
          return handleUnion(value, fieldSchema);
        case RECORD:
          return jsonMapToGenericRecord((Map<String, Object>) value, fieldSchema);
        case STRING, ENUM:
          return value.toString();
        case INT:
          return convertToInt(value);
        case LONG:
          return convertToLong(value);
        case FLOAT:
          return convertToFloat(value);
        case DOUBLE:
          return convertToDouble(value);
        case BOOLEAN:
          return convertToBoolean(value);
        case BYTES:
          return convertToBytes(value);
        case ARRAY:
          return convertArray(value, fieldSchema);
        case FIXED:
          throw new UnsupportedOperationException("FIXED type conversion not implemented yet");
        case NULL:
          return null;
        default:
          throw new IllegalArgumentException("Unsupported Avro type: " + fieldSchema.getType());
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Conversion error for value '"
              + value
              + "' to type "
              + fieldSchema.getType()
              + ": "
              + e.getMessage(),
          e);
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
          return convertValue(value, type);
        } catch (Exception e) {
          System.err.println(
              "Failed to convert value '"
                  + value
                  + "' to type "
                  + type
                  + " in union: "
                  + e.getMessage());
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

  private static Object convertArray(Object value, Schema fieldSchema) {
    Schema elementSchema = fieldSchema.getElementType();
    if (!(value instanceof List)) {
      throw new IllegalArgumentException(
          "Expected a List for ARRAY type, but got " + value.getClass().getName());
    }
    List<?> list = (List<?>) value;
    List<Object> convertedList = new ArrayList<>();
    for (Object element : list) {
      convertedList.add(convertValue(element, elementSchema));
    }
    return convertedList;
  }

  private static boolean isNullable(Schema fieldSchema) {
    return fieldSchema.getType() == Schema.Type.UNION
        && fieldSchema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
  }

  private static Integer convertToInt(Object value) {
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

  private static Long convertToLong(Object value) {
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

  private static Float convertToFloat(Object value) {
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

  private static Double convertToDouble(Object value) {
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

  private static Boolean convertToBoolean(Object value) {
    if (value instanceof Boolean) {
      return (Boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " to Boolean");
  }

  private static byte[] convertToBytes(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    } else if (value instanceof String) {
      return ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    } else if (value instanceof ByteBuffer buffer) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    }
    throw new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " to byte[]");
  }
}
