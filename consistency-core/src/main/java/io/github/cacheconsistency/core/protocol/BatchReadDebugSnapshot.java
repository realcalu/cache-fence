package io.github.cacheconsistency.core.protocol;

/**
 * Structured debug view for one key processed by the batch read protocol.
 */
public final class BatchReadDebugSnapshot {
    public enum Decision {
        CACHE_HIT,
        INVALID_CACHE,
        LAST_WRITE_SUCCESS,
        IN_WRITING,
        STATUS_MISSING
    }

    private final String businessKey;
    private final String dataKey;
    private final String statusKey;
    private final String validityKey;
    private final boolean dataPresent;
    private final ProtocolState state;
    private final ProtocolValidity validity;
    private final Decision decision;

    public BatchReadDebugSnapshot(String businessKey,
                                  String dataKey,
                                  String statusKey,
                                  String validityKey,
                                  boolean dataPresent,
                                  ProtocolState state,
                                  ProtocolValidity validity,
                                  Decision decision) {
        this.businessKey = businessKey;
        this.dataKey = dataKey;
        this.statusKey = statusKey;
        this.validityKey = validityKey;
        this.dataPresent = dataPresent;
        this.state = state;
        this.validity = validity;
        this.decision = decision;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public String getDataKey() {
        return dataKey;
    }

    public String getStatusKey() {
        return statusKey;
    }

    public String getValidityKey() {
        return validityKey;
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

    public Decision getDecision() {
        return decision;
    }
}
