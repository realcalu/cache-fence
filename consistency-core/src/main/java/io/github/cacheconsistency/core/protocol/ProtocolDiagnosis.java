package io.github.cacheconsistency.core.protocol;

/**
 * Human-readable diagnosis derived from a protocol snapshot.
 */
public final class ProtocolDiagnosis {
    private final ProtocolSnapshot snapshot;
    private final String summary;
    private final String recommendedAction;

    public ProtocolDiagnosis(ProtocolSnapshot snapshot, String summary, String recommendedAction) {
        this.snapshot = snapshot;
        this.summary = summary;
        this.recommendedAction = recommendedAction;
    }

    public ProtocolSnapshot getSnapshot() {
        return snapshot;
    }

    public String getSummary() {
        return summary;
    }

    public String getRecommendedAction() {
        return recommendedAction;
    }

    /**
     * Renders a compact single-line summary that is suitable for logs or debug endpoints.
     */
    public String format() {
        return "summary=" + summary
                + ", recommendedAction=" + recommendedAction
                + ", dataPresent=" + snapshot.isDataPresent()
                + ", state=" + snapshot.getState()
                + ", validity=" + snapshot.getValidity()
                + ", readLeaseToken=" + snapshot.getReadLeaseToken()
                + ", writeLeaseToken=" + snapshot.getWriteLeaseToken();
    }
}
