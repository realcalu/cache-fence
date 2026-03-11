package io.github.cacheconsistency.core;

/**
 * One item in a batch delete request with its own context.
 */
public final class BatchDeleteCommand {
    private final String key;
    private final ConsistencyContext context;

    public BatchDeleteCommand(String key, ConsistencyContext context) {
        this.key = key;
        this.context = context;
    }

    public static BatchDeleteCommand of(String key, ConsistencyContext context) {
        return new BatchDeleteCommand(key, context);
    }

    public String getKey() {
        return key;
    }

    public ConsistencyContext getContext() {
        return context;
    }
}
