package io.github.cacheconsistency.core.protocol;

/**
 * Read-only view of the protocol state currently stored in Redis for one business key.
 */
public final class ProtocolSnapshot {
    private final String businessKey;
    private final boolean dataPresent;
    private final ProtocolState state;
    private final ProtocolValidity validity;
    private final String readLeaseToken;
    private final String writeLeaseToken;

    public ProtocolSnapshot(String businessKey,
                            boolean dataPresent,
                            ProtocolState state,
                            ProtocolValidity validity,
                            String readLeaseToken,
                            String writeLeaseToken) {
        this.businessKey = businessKey;
        this.dataPresent = dataPresent;
        this.state = state;
        this.validity = validity;
        this.readLeaseToken = readLeaseToken;
        this.writeLeaseToken = writeLeaseToken;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public boolean isDataPresent() {
        return dataPresent;
    }

    public ProtocolState getState() {
        return state;
    }

    public ProtocolValidity getValidity() {
        return validity;
    }

    public String getReadLeaseToken() {
        return readLeaseToken;
    }

    public String getWriteLeaseToken() {
        return writeLeaseToken;
    }

    /**
     * Returns whether the snapshot represents a cache entry that can be safely served.
     */
    public boolean isCacheReadable() {
        return dataPresent && validity != ProtocolValidity.INVALID;
    }

    /**
     * Returns whether the protocol indicates that a write is still in flight.
     */
    public boolean isWriteInFlight() {
        return state == ProtocolState.IN_WRITING;
    }

    /**
     * Returns whether protocol metadata is missing in a way that suggests a ghost write window.
     */
    public boolean isGhostWriteSuspected() {
        return !dataPresent && state == null;
    }
}
