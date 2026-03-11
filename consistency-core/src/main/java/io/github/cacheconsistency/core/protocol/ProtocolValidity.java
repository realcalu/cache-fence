package io.github.cacheconsistency.core.protocol;

/**
 * Public cache-validity flag used by the Redis protocol.
 */
public enum ProtocolValidity {
    VALID("1"),
    INVALID("0");

    private final String value;

    ProtocolValidity(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static ProtocolValidity fromValue(String value) {
        for (ProtocolValidity validity : values()) {
            if (validity.value.equals(value)) {
                return validity;
            }
        }
        return null;
    }
}
