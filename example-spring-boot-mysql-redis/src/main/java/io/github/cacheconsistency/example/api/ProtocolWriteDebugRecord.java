package io.github.cacheconsistency.example.api;

import io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot;

/**
 * One in-memory write debug record captured by the demo.
 */
public class ProtocolWriteDebugRecord {
    private final long sequence;
    private final long recordedAtEpochMillis;
    private final BatchWriteDebugSnapshot snapshot;

    public ProtocolWriteDebugRecord(long sequence, long recordedAtEpochMillis, BatchWriteDebugSnapshot snapshot) {
        this.sequence = sequence;
        this.recordedAtEpochMillis = recordedAtEpochMillis;
        this.snapshot = snapshot;
    }

    public long getSequence() {
        return sequence;
    }

    public long getRecordedAtEpochMillis() {
        return recordedAtEpochMillis;
    }

    public BatchWriteDebugSnapshot getSnapshot() {
        return snapshot;
    }
}
