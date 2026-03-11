package io.github.cacheconsistency.core.protocol;

/**
 * Public write states used by the Redis protocol.
 */
public enum ProtocolState {
    IN_WRITING("IN_WRITING"),
    LAST_WRITE_SUCCESS("LAST_WRITE_SUCCESS");

    private final String value;

    ProtocolState(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    public static ProtocolState fromValue(String value) {
        for (ProtocolState state : values()) {
            if (state.value.equals(value)) {
                return state;
            }
        }
        return null;
    }
}
