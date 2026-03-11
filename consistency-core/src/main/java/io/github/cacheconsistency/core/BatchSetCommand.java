package io.github.cacheconsistency.core;

/**
 * One item in a batch write request with its own context.
 */
public final class BatchSetCommand<T> {
    private final String key;
    private final T value;
    private final ConsistencyContext context;

    public BatchSetCommand(String key, T value, ConsistencyContext context) {
        this.key = key;
        this.value = value;
        this.context = context;
    }

    public static <T> BatchSetCommand<T> of(String key, T value, ConsistencyContext context) {
        return new BatchSetCommand<T>(key, value, context);
    }

    public String getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }

    public ConsistencyContext getContext() {
        return context;
    }
}
