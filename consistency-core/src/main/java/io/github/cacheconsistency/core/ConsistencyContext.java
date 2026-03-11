package io.github.cacheconsistency.core;

import io.github.cacheconsistency.core.failover.FailoverStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * Mutable protocol context passed across a single client call.
 *
 * <p>The context carries business key, expected version, command type and optional
 * attachments so callers can thread custom metadata through their store adapter.</p>
 */
public class ConsistencyContext {
    private final Map<String, Object> attachments = new HashMap<String, Object>();
    private String key;
    private Object value;
    private Long ttlSeconds;
    private String version;
    private CommandType commandType;
    private FailoverStrategy failoverStrategy;

    public static ConsistencyContext create() {
        return new ConsistencyContext();
    }

    public static ConsistencyContext merge(ConsistencyContext base, ConsistencyContext override) {
        if (base == null && override == null) {
            return ConsistencyContext.create();
        }
        ConsistencyContext result = base == null ? ConsistencyContext.create() : base.copy();
        if (override == null) {
            return result;
        }
        result.attachments.putAll(override.attachments);
        if (override.key != null) {
            result.key = override.key;
        }
        if (override.value != null) {
            result.value = override.value;
        }
        if (override.ttlSeconds != null) {
            result.ttlSeconds = override.ttlSeconds;
        }
        if (override.version != null) {
            result.version = override.version;
        }
        if (override.commandType != null) {
            result.commandType = override.commandType;
        }
        if (override.failoverStrategy != null) {
            result.failoverStrategy = override.failoverStrategy;
        }
        return result;
    }

    /**
     * Creates a shallow copy so retries and batch operations can reuse the same logical input
     * without sharing mutable state.
     */
    public ConsistencyContext copy() {
        ConsistencyContext context = new ConsistencyContext();
        context.attachments.putAll(this.attachments);
        context.key = this.key;
        context.value = this.value;
        context.ttlSeconds = this.ttlSeconds;
        context.version = this.version;
        context.commandType = this.commandType;
        context.failoverStrategy = this.failoverStrategy;
        return context;
    }

    public ConsistencyContext withKey(String key) {
        this.key = key;
        return this;
    }

    public ConsistencyContext withValue(Object value) {
        this.value = value;
        return this;
    }

    public ConsistencyContext withTtlSeconds(Long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
        return this;
    }

    public ConsistencyContext withVersion(String version) {
        this.version = version;
        return this;
    }

    public ConsistencyContext withCommandType(CommandType commandType) {
        this.commandType = commandType;
        return this;
    }

    public ConsistencyContext withFailoverStrategy(FailoverStrategy failoverStrategy) {
        this.failoverStrategy = failoverStrategy;
        return this;
    }

    public ConsistencyContext putAttachment(String key, Object value) {
        attachments.put(key, value);
        return this;
    }

    public Object getAttachment(String key) {
        return attachments.get(key);
    }

    public String getKey() {
        return key;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue() {
        return (T) value;
    }

    public Long getTtlSeconds() {
        return ttlSeconds;
    }

    public String getVersion() {
        return version;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public FailoverStrategy getFailoverStrategy() {
        return failoverStrategy;
    }
}
