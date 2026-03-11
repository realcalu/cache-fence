package io.github.cacheconsistency.core.protocol;

/**
 * Helper for building the Redis keys that make up the protocol state for one business key.
 */
public final class ProtocolKeys {
    private final String prefix;

    public ProtocolKeys(String prefix) {
        this.prefix = prefix;
    }

    public String data(String key) {
        return join(ProtocolConstants.DATA_SEGMENT, key);
    }

    public String status(String key) {
        return join(ProtocolConstants.STATUS_SEGMENT, key);
    }

    public String validity(String key) {
        return join(ProtocolConstants.VALIDITY_SEGMENT, key);
    }

    public String lease(String key) {
        return join(ProtocolConstants.LEASE_SEGMENT, key);
    }

    public String protocolLease(String key) {
        return join(ProtocolConstants.PROTOCOL_LEASE_SEGMENT, key);
    }

    private String join(String segment, String key) {
        return prefix + ":" + segment + ":" + key;
    }
}
