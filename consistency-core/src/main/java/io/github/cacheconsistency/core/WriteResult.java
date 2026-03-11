package io.github.cacheconsistency.core;

/**
 * Write outcome returned by the protocol client.
 */
public final class WriteResult {
    /**
     * High-level protocol result states for writes and deletes.
     */
    public enum WriteStatus {
        STORE_UPDATED,
        STORE_UPDATED_CACHE_REPAIR_SCHEDULED,
        STORE_AND_CACHE_UPDATED,
        DELETED,
        DELETED_CACHE_REPAIR_SCHEDULED,
        VERSION_REJECTED,
        WRITE_LOCK_BUSY
    }

    private final WriteStatus status;

    private WriteResult(WriteStatus status) {
        this.status = status;
    }

    public static WriteResult of(WriteStatus status) {
        return new WriteResult(status);
    }

    public WriteStatus getStatus() {
        return status;
    }
}
