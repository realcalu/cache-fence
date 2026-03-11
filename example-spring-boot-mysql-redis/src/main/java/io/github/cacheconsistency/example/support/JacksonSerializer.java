package io.github.cacheconsistency.example.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cacheconsistency.core.Serializer;

public class JacksonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper;
    private final Class<T> type;

    public JacksonSerializer(ObjectMapper objectMapper, Class<T> type) {
        this.objectMapper = objectMapper;
        this.type = type;
    }

    @Override
    public byte[] serialize(T value) {
        try {
            return value == null ? null : objectMapper.writeValueAsBytes(value);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to serialize value", error);
        }
    }

    @Override
    public T deserialize(byte[] bytes) {
        try {
            return bytes == null ? null : objectMapper.readValue(bytes, type);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to deserialize value", error);
        }
    }
}
