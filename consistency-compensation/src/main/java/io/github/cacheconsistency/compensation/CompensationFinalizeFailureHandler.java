package io.github.cacheconsistency.compensation;

import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.support.FinalizeFailureHandler;

import java.time.Duration;

/**
 * Adapts the optional compensation module to the core finalize-failure extension hook.
 */
public final class CompensationFinalizeFailureHandler implements FinalizeFailureHandler {
    private final CompensationExecutor compensationExecutor;

    public CompensationFinalizeFailureHandler(CompensationExecutor compensationExecutor) {
        this.compensationExecutor = compensationExecutor;
    }

    @Override
    public void onSetFinalizeFailure(String key,
                                     String cacheKey,
                                     byte[] cacheValue,
                                     Duration ttl,
                                     ConsistencyContext context,
                                     RuntimeException error) {
        compensationExecutor.scheduleCacheWrite(cacheKey, cacheValue, ttl);
    }

    @Override
    public void onDeleteFinalizeFailure(String key,
                                        String cacheKey,
                                        ConsistencyContext context,
                                        RuntimeException error) {
        compensationExecutor.scheduleCacheDelete(cacheKey);
    }
}
