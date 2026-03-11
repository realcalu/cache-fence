package io.github.cacheconsistency.compensation;

import java.time.Duration;

/**
 * Optional extension for scheduling cache-side retries outside the core protocol path.
 */
public interface CompensationExecutor {
    void scheduleCacheWrite(String cacheKey, byte[] value, Duration ttl);

    void scheduleCacheDelete(String cacheKey);

    final class NoOpCompensationExecutor implements CompensationExecutor {
        public static final NoOpCompensationExecutor INSTANCE = new NoOpCompensationExecutor();

        private NoOpCompensationExecutor() {
        }

        @Override
        public void scheduleCacheWrite(String cacheKey, byte[] value, Duration ttl) {
        }

        @Override
        public void scheduleCacheDelete(String cacheKey) {
        }
    }
}
