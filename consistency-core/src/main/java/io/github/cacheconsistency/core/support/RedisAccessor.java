package io.github.cacheconsistency.core.support;

import java.time.Duration;

/**
 * Redis contract used by the protocol client.
 *
 * <p>The abstraction keeps protocol semantics explicit while allowing different Redis drivers
 * or test doubles to provide the underlying implementation.</p>
 */
public interface RedisAccessor {
    byte[] get(String key);

    void set(String key, byte[] value, Duration ttl);

    boolean setIfAbsent(String key, byte[] value, Duration ttl);

    boolean compareAndDelete(String key, byte[] expectedValue);

    boolean beginWrite(String statusKey,
                       String protocolLeaseKey,
                       String dataKey,
                       String validityKey,
                       byte[] leaseToken,
                       Duration writeWindowTtl);

    boolean stageCacheValue(String protocolLeaseKey,
                            byte[] expectedValue,
                            String cacheKey,
                            byte[] cacheValue,
                            Duration ttl);

    boolean writeCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl);

    boolean deleteCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey);

    boolean finalizeWrite(String statusKey,
                          String protocolLeaseKey,
                          String validityKey,
                          String dataKey,
                          byte[] leaseToken,
                          Duration statusTtl,
                          Duration dataTtl);

    boolean rollbackWrite(String statusKey,
                          String protocolLeaseKey,
                          String validityKey,
                          byte[] leaseToken);

    long delete(String... keys);

    void close();
}
