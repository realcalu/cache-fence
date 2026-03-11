package io.github.cacheconsistency.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized Spring Boot properties for the consistency client.
 */
@ConfigurationProperties(prefix = "cck")
public class CacheConsistencyProperties {
    private long leaseTtlSeconds = 5;
    private long writeLockTtlSeconds = 5;
    private long retryBackoffMillis = 50;
    private boolean ghostWriteHealingEnabled = false;
    private int batchParallelism = 1;
    private String keyPrefix = "cck";
    private final Metrics metrics = new Metrics();
    private final Compensation compensation = new Compensation();

    public long getLeaseTtlSeconds() {
        return leaseTtlSeconds;
    }

    public void setLeaseTtlSeconds(long leaseTtlSeconds) {
        this.leaseTtlSeconds = leaseTtlSeconds;
    }

    public long getWriteLockTtlSeconds() {
        return writeLockTtlSeconds;
    }

    public void setWriteLockTtlSeconds(long writeLockTtlSeconds) {
        this.writeLockTtlSeconds = writeLockTtlSeconds;
    }

    public long getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public void setRetryBackoffMillis(long retryBackoffMillis) {
        this.retryBackoffMillis = retryBackoffMillis;
    }

    public boolean isGhostWriteHealingEnabled() {
        return ghostWriteHealingEnabled;
    }

    public void setGhostWriteHealingEnabled(boolean ghostWriteHealingEnabled) {
        this.ghostWriteHealingEnabled = ghostWriteHealingEnabled;
    }

    public int getBatchParallelism() {
        return batchParallelism;
    }

    public void setBatchParallelism(int batchParallelism) {
        this.batchParallelism = batchParallelism;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    public Compensation getCompensation() {
        return compensation;
    }

    public static final class Metrics {
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static final class Compensation {
        private boolean enabled = false;
        private int maxRetries = 3;
        private long retryDelayMillis = 500L;
        private String storePath = "runtime/cck-compensation.log";
        private boolean replayPendingOnStartup = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public long getRetryDelayMillis() {
            return retryDelayMillis;
        }

        public void setRetryDelayMillis(long retryDelayMillis) {
            this.retryDelayMillis = retryDelayMillis;
        }

        public String getStorePath() {
            return storePath;
        }

        public void setStorePath(String storePath) {
            this.storePath = storePath;
        }

        public boolean isReplayPendingOnStartup() {
            return replayPendingOnStartup;
        }

        public void setReplayPendingOnStartup(boolean replayPendingOnStartup) {
            this.replayPendingOnStartup = replayPendingOnStartup;
        }
    }
}
