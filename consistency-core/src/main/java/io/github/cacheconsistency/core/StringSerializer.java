package io.github.cacheconsistency.core;

import java.nio.charset.StandardCharsets;

/**
 * UTF-8 serializer for plain strings.
 */
public final class StringSerializer implements Serializer<String> {
    public static final StringSerializer UTF8 = new StringSerializer();

    private StringSerializer() {
    }

    @Override
    public byte[] serialize(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String deserialize(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
