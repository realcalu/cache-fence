package io.github.cacheconsistency.core;

/**
 * Serialization contract used for cache payloads.
 */
public interface Serializer<T> {
    byte[] serialize(T value);

    T deserialize(byte[] bytes);
}
