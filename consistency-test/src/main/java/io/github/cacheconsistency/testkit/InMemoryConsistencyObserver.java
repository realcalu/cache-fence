package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.support.ConsistencyObserver;

import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryConsistencyObserver implements ConsistencyObserver {
    private final AtomicInteger cacheHits = new AtomicInteger();
    private final AtomicInteger cacheMisses = new AtomicInteger();
    private final AtomicInteger compensations = new AtomicInteger();

    @Override
    public void onCacheHit(String key) {
        cacheHits.incrementAndGet();
    }

    @Override
    public void onCacheMiss(String key) {
        cacheMisses.incrementAndGet();
    }

    @Override
    public void onLeaseAcquired(String key) {
    }

    @Override
    public void onStoreRead(String key) {
    }

    @Override
    public void onStoreWrite(String key) {
    }

    @Override
    public void onStoreDelete(String key) {
    }

    @Override
    public void onVersionRejected(String key) {
    }

    @Override
    public void onFinalizeFailure(String key, String action, Throwable error) {
    }

    @Override
    public void onGhostWriteHealScheduled(String key) {
    }

    @Override
    public void onGhostWriteHealSuccess(String key) {
    }

    @Override
    public void onGhostWriteHealFailure(String key, Throwable error) {
    }

    @Override
    public void onBatchOperation(String action, int size, boolean optimized) {
    }

    @Override
    public void onCompensationScheduled(String key, String action) {
        compensations.incrementAndGet();
    }

    @Override
    public void onCompensationSuccess(String key, String action, int attempt) {
    }

    @Override
    public void onCompensationFailure(String key, String action, int attempt, Throwable error) {
    }

    public int getCacheHits() {
        return cacheHits.get();
    }

    public int getCacheMisses() {
        return cacheMisses.get();
    }

    public int getCompensations() {
        return compensations.get();
    }
}
