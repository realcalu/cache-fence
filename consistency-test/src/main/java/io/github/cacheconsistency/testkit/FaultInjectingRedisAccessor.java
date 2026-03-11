package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.support.RedisAccessor;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class FaultInjectingRedisAccessor implements RedisAccessor {
    private final RedisAccessor delegate;
    private final AtomicInteger getFailuresRemaining = new AtomicInteger();
    private final AtomicInteger setFailuresRemaining = new AtomicInteger();
    private final AtomicInteger beginWriteFailuresRemaining = new AtomicInteger();

    public FaultInjectingRedisAccessor(RedisAccessor delegate) {
        this.delegate = delegate;
    }

    public void failNextGets(int count) {
        getFailuresRemaining.set(count);
    }

    public void failNextSets(int count) {
        setFailuresRemaining.set(count);
    }

    public void failNextBeginWrites(int count) {
        beginWriteFailuresRemaining.set(count);
    }

    @Override
    public byte[] get(String key) {
        failIfNeeded(getFailuresRemaining, "Injected Redis get failure");
        return delegate.get(key);
    }

    @Override
    public void set(String key, byte[] value, Duration ttl) {
        failIfNeeded(setFailuresRemaining, "Injected Redis set failure");
        delegate.set(key, value, ttl);
    }

    @Override
    public boolean setIfAbsent(String key, byte[] value, Duration ttl) {
        failIfNeeded(setFailuresRemaining, "Injected Redis setIfAbsent failure");
        return delegate.setIfAbsent(key, value, ttl);
    }

    @Override
    public boolean compareAndDelete(String key, byte[] expectedValue) {
        return delegate.compareAndDelete(key, expectedValue);
    }

    @Override
    public boolean beginWrite(String statusKey,
                              String protocolLeaseKey,
                              String dataKey,
                              String validityKey,
                              byte[] leaseToken,
                              Duration writeWindowTtl) {
        failIfNeeded(beginWriteFailuresRemaining, "Injected Redis beginWrite failure");
        return delegate.beginWrite(statusKey, protocolLeaseKey, dataKey, validityKey, leaseToken, writeWindowTtl);
    }

    @Override
    public boolean stageCacheValue(String protocolLeaseKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) {
        return delegate.stageCacheValue(protocolLeaseKey, expectedValue, cacheKey, cacheValue, ttl);
    }

    @Override
    public boolean writeCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) {
        return delegate.writeCacheAndReleaseLock(lockKey, expectedValue, cacheKey, cacheValue, ttl);
    }

    @Override
    public boolean deleteCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey) {
        return delegate.deleteCacheAndReleaseLock(lockKey, expectedValue, cacheKey);
    }

    @Override
    public boolean finalizeWrite(String statusKey, String protocolLeaseKey, String validityKey, String dataKey, byte[] leaseToken, Duration statusTtl, Duration dataTtl) {
        return delegate.finalizeWrite(statusKey, protocolLeaseKey, validityKey, dataKey, leaseToken, statusTtl, dataTtl);
    }

    @Override
    public boolean rollbackWrite(String statusKey, String protocolLeaseKey, String validityKey, byte[] leaseToken) {
        return delegate.rollbackWrite(statusKey, protocolLeaseKey, validityKey, leaseToken);
    }

    @Override
    public long delete(String... keys) {
        return delegate.delete(keys);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private void failIfNeeded(AtomicInteger counter, String message) {
        while (true) {
            int current = counter.get();
            if (current <= 0) {
                return;
            }
            if (counter.compareAndSet(current, current - 1)) {
                throw new RuntimeException(message);
            }
        }
    }
}
