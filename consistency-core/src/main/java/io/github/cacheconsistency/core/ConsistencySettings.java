package io.github.cacheconsistency.core;

/**
 * Immutable settings that shape protocol timing and optional extensions.
 */
public final class ConsistencySettings {
    private final long leaseTtlSeconds;
    private final long writeLockTtlSeconds;
    private final long retryBackoffMillis;
    private final boolean ghostWriteHealingEnabled;
    private final int batchParallelism;
    private final String keyPrefix;

    private ConsistencySettings(Builder builder) {
        this.leaseTtlSeconds = builder.leaseTtlSeconds;
        this.writeLockTtlSeconds = builder.writeLockTtlSeconds;
        this.retryBackoffMillis = builder.retryBackoffMillis;
        this.ghostWriteHealingEnabled = builder.ghostWriteHealingEnabled;
        this.batchParallelism = builder.batchParallelism;
        this.keyPrefix = builder.keyPrefix;
    }

    public static Builder builder() {
        return new Builder();
    }

    public long getLeaseTtlSeconds() {
        return leaseTtlSeconds;
    }

    public long getWriteLockTtlSeconds() {
        return writeLockTtlSeconds;
    }

    public long getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public boolean isGhostWriteHealingEnabled() {
        return ghostWriteHealingEnabled;
    }

    public int getBatchParallelism() {
        return batchParallelism;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public static final class Builder {
        private long leaseTtlSeconds = 5;
        private long writeLockTtlSeconds = 5;
        private long retryBackoffMillis = 50;
        private boolean ghostWriteHealingEnabled = false;
        private int batchParallelism = 1;
        private String keyPrefix = "cck";

        public Builder leaseTtlSeconds(long leaseTtlSeconds) {
            this.leaseTtlSeconds = leaseTtlSeconds;
            return this;
        }

        public Builder writeLockTtlSeconds(long writeLockTtlSeconds) {
            this.writeLockTtlSeconds = writeLockTtlSeconds;
            return this;
        }

        public Builder retryBackoffMillis(long retryBackoffMillis) {
            this.retryBackoffMillis = retryBackoffMillis;
            return this;
        }

        public Builder ghostWriteHealingEnabled(boolean ghostWriteHealingEnabled) {
            this.ghostWriteHealingEnabled = ghostWriteHealingEnabled;
            return this;
        }

        public Builder batchParallelism(int batchParallelism) {
            this.batchParallelism = batchParallelism;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
            return this;
        }

        /**
         * Builds validated settings for a client instance.
         */
        public ConsistencySettings build() {
            if (leaseTtlSeconds <= 0 || writeLockTtlSeconds <= 0) {
                throw new IllegalArgumentException("Lease and write lock TTL must be greater than 0");
            }
            if (retryBackoffMillis < 0) {
                throw new IllegalArgumentException("Retry backoff must be greater than or equal to 0");
            }
            if (batchParallelism <= 0) {
                throw new IllegalArgumentException("Batch parallelism must be greater than 0");
            }
            if (keyPrefix == null || keyPrefix.trim().isEmpty()) {
                throw new IllegalArgumentException("Key prefix must not be blank");
            }
            return new ConsistencySettings(this);
        }
    }
}
