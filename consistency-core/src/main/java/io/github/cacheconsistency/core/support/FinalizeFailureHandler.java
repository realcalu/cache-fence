package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.ConsistencyContext;

import java.time.Duration;

/**
 * Optional extension hook that runs only when the Redis finalize step fails after the store mutation succeeded.
 */
public interface FinalizeFailureHandler {
    void onSetFinalizeFailure(String key,
                              String cacheKey,
                              byte[] cacheValue,
                              Duration ttl,
                              ConsistencyContext context,
                              RuntimeException error);

    void onDeleteFinalizeFailure(String key,
                                 String cacheKey,
                                 ConsistencyContext context,
                                 RuntimeException error);

    final class NoOpFinalizeFailureHandler implements FinalizeFailureHandler {
        public static final NoOpFinalizeFailureHandler INSTANCE = new NoOpFinalizeFailureHandler();

        private NoOpFinalizeFailureHandler() {
        }

        @Override
        public void onSetFinalizeFailure(String key,
                                         String cacheKey,
                                         byte[] cacheValue,
                                         Duration ttl,
                                         ConsistencyContext context,
                                         RuntimeException error) {
        }

        @Override
        public void onDeleteFinalizeFailure(String key,
                                            String cacheKey,
                                            ConsistencyContext context,
                                            RuntimeException error) {
        }
    }
}
