package io.github.cacheconsistency.core.failover;

import io.github.cacheconsistency.core.ConsistencyContext;

/**
 * Strategy hook for callers that want to relax or tighten behavior after cache-side failures.
 */
public interface FailoverStrategy {
    default boolean allowStoreReadOnCacheFailure(ConsistencyContext context, RuntimeException error) {
        return true;
    }

    default boolean allowCacheWriteSkipOnFailure(ConsistencyContext context, RuntimeException error) {
        return true;
    }

    default boolean allowStoreWriteRetry(ConsistencyContext context, RuntimeException error) {
        return false;
    }
}
