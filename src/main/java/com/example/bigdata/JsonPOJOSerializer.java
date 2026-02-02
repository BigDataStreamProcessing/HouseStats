package com.example.bigdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* Source:
https://github.com/apache/kafka/blob/2.0/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/JsonPOJOSerializer.java
 */

public class JsonPOJOSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean includeSchema = false;
    private Class<T> tClass;

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> props, boolean isKey) {
        if (props.containsKey("include.schema")) {
            this.includeSchema = (boolean) props.get("include.schema");
        }
        this.tClass = (Class<T>) props.get("JsonPOJOClass");
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            if (includeSchema) {
                // Jeśli flaga jest ON, budujemy "kopertę" dla Connecta
                Map<String, Object> envelope = new HashMap<>();
                Map<String, Object> schema = new HashMap<>();
                schema.put("type", "struct");
                schema.put("optional", true);

                // --- DYNAMIKCZNE GENEROWANIE PÓL (Refleksja) ---
                List<Map<String, Object>> fields = getMaps();
                // -----------------------------------------------
                schema.put("fields", fields);

                envelope.put("schema", schema);
                envelope.put("payload", data);
                return objectMapper.writeValueAsBytes(envelope);
            } else {
                return objectMapper.writeValueAsBytes(data);
            }
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    private List<Map<String, Object>> getMaps() {
        List<Map<String, Object>> fields = new ArrayList<>();
        for (Field field : tClass.getDeclaredFields()) {
            Map<String, Object> fieldMap = new HashMap<>();
            fieldMap.put("field", field.getName());
            fieldMap.put("optional", true);

            // Mapowanie typów Javy na typy Kafka Connect
            String type = field.getType().getSimpleName().toLowerCase();
            switch (type) {
                case "int":
                case "integer": fieldMap.put("type", "int32"); break;
                case "long": fieldMap.put("type", "int64"); break;
                case "double":
                case "float":   fieldMap.put("type", "double"); break;
                case "boolean": fieldMap.put("type", "boolean"); break;
                default:        fieldMap.put("type", "string"); break;
            }
            fields.add(fieldMap);
        }
        return fields;
    }

    @Override
    public void close() {
    }

}