package io.github.cacheconsistency.spring;

import io.github.cacheconsistency.core.support.ConsistencyObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer-backed observer that turns protocol events into counters.
 */
public final class MicrometerConsistencyObserver implements ConsistencyObserver {
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;
    private final Counter leaseAcquiredCounter;
    private final Counter storeReadCounter;
    private final Counter storeWriteCounter;
    private final Counter storeDeleteCounter;
    private final Counter versionRejectedCounter;
    private final Counter finalizeFailureCounter;
    private final Counter ghostWriteHealScheduledCounter;
    private final Counter ghostWriteHealSuccessCounter;
    private final Counter ghostWriteHealFailureCounter;
    private final Counter compensationScheduledCounter;
    private final Counter compensationSuccessCounter;
    private final Counter compensationFailureCounter;
    private final MeterRegistry meterRegistry;

    public MicrometerConsistencyObserver(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.cacheHitCounter = meterRegistry.counter("cck.cache.hit");
        this.cacheMissCounter = meterRegistry.counter("cck.cache.miss");
        this.leaseAcquiredCounter = meterRegistry.counter("cck.lease.acquired");
        this.storeReadCounter = meterRegistry.counter("cck.store.read");
        this.storeWriteCounter = meterRegistry.counter("cck.store.write");
        this.storeDeleteCounter = meterRegistry.counter("cck.store.delete");
        this.versionRejectedCounter = meterRegistry.counter("cck.version.rejected");
        this.finalizeFailureCounter = meterRegistry.counter("cck.finalize.failure");
        this.ghostWriteHealScheduledCounter = meterRegistry.counter("cck.ghost_heal.scheduled");
        this.ghostWriteHealSuccessCounter = meterRegistry.counter("cck.ghost_heal.success");
        this.ghostWriteHealFailureCounter = meterRegistry.counter("cck.ghost_heal.failure");
        this.compensationScheduledCounter = meterRegistry.counter("cck.compensation.scheduled");
        this.compensationSuccessCounter = meterRegistry.counter("cck.compensation.success");
        this.compensationFailureCounter = meterRegistry.counter("cck.compensation.failure");
    }

    @Override
    public void onCacheHit(String key) {
        cacheHitCounter.increment();
    }

    @Override
    public void onCacheMiss(String key) {
        cacheMissCounter.increment();
    }

    @Override
    public void onLeaseAcquired(String key) {
        leaseAcquiredCounter.increment();
    }

    @Override
    public void onStoreRead(String key) {
        storeReadCounter.increment();
    }

    @Override
    public void onStoreWrite(String key) {
        storeWriteCounter.increment();
    }

    @Override
    public void onStoreDelete(String key) {
        storeDeleteCounter.increment();
    }

    @Override
    public void onVersionRejected(String key) {
        versionRejectedCounter.increment();
    }

    @Override
    public void onFinalizeFailure(String key, String action, Throwable error) {
        finalizeFailureCounter.increment();
    }

    @Override
    public void onGhostWriteHealScheduled(String key) {
        ghostWriteHealScheduledCounter.increment();
    }

    @Override
    public void onGhostWriteHealSuccess(String key) {
        ghostWriteHealSuccessCounter.increment();
    }

    @Override
    public void onGhostWriteHealFailure(String key, Throwable error) {
        ghostWriteHealFailureCounter.increment();
    }

    @Override
    public void onBatchOperation(String action, int size, boolean optimized) {
        meterRegistry.counter("cck.batch.invocation", "action", action, "optimized", String.valueOf(optimized)).increment();
        meterRegistry.counter("cck.batch.items", "action", action, "optimized", String.valueOf(optimized)).increment(size);
    }

    @Override
    public void onCompensationScheduled(String key, String action) {
        compensationScheduledCounter.increment();
    }

    @Override
    public void onCompensationSuccess(String key, String action, int attempt) {
        compensationSuccessCounter.increment();
    }

    @Override
    public void onCompensationFailure(String key, String action, int attempt, Throwable error) {
        compensationFailureCounter.increment();
    }
}
