package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot;
import io.github.cacheconsistency.core.support.BatchRedisAccessor;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryRedisAccessor implements BatchRedisAccessor {
    private final Map<String, Entry> storage = new ConcurrentHashMap<String, Entry>();
    private volatile int batchBeginWriteCount;
    private volatile int batchStageCacheCount;
    private volatile int batchFinalizeCount;
    private volatile int batchReadProtocolCount;

    @Override
    public byte[] get(String key) {
        Entry entry = storage.get(key);
        if (entry == null) {
            return null;
        }
        if (entry.isExpired()) {
            storage.remove(key);
            return null;
        }
        return entry.value;
    }

    @Override
    public void set(String key, byte[] value, Duration ttl) {
        storage.put(key, new Entry(value, expireAt(ttl)));
    }

    @Override
    public Map<String, byte[]> getAll(Collection<String> keys) {
        Map<String, byte[]> result = new LinkedHashMap<String, byte[]>();
        for (String key : keys) {
            result.put(key, get(key));
        }
        return result;
    }

    @Override
    public void setAll(Map<String, byte[]> values, Duration ttl) {
        for (Map.Entry<String, byte[]> entry : values.entrySet()) {
            set(entry.getKey(), entry.getValue(), ttl);
        }
    }

    @Override
    public Map<String, Boolean> beginWriteAll(Collection<BeginWriteRequest> requests) {
        batchBeginWriteCount++;
        Map<String, Boolean> result = new LinkedHashMap<String, Boolean>();
        for (BeginWriteRequest request : requests) {
            result.put(request.getKey(), beginWrite(
                    request.getStatusKey(),
                    request.getProtocolLeaseKey(),
                    request.getDataKey(),
                    request.getValidityKey(),
                    request.getLeaseToken(),
                    request.getWriteWindowTtl()
            ));
        }
        return result;
    }

    @Override
    public Map<String, Boolean> stageCacheValueAll(Collection<StageCacheValueRequest> requests) {
        batchStageCacheCount++;
        Map<String, Boolean> result = new LinkedHashMap<String, Boolean>();
        for (StageCacheValueRequest request : requests) {
            result.put(request.getKey(), stageCacheValue(
                    request.getProtocolLeaseKey(),
                    request.getExpectedValue(),
                    request.getCacheKey(),
                    request.getCacheValue(),
                    request.getTtl()
            ));
        }
        return result;
    }

    @Override
    public Map<String, Boolean> finalizeWriteAll(Collection<FinalizeWriteRequest> requests) {
        batchFinalizeCount++;
        Map<String, Boolean> result = new LinkedHashMap<String, Boolean>();
        for (FinalizeWriteRequest request : requests) {
            result.put(request.getKey(), finalizeWrite(
                    request.getStatusKey(),
                    request.getProtocolLeaseKey(),
                    request.getValidityKey(),
                    request.getDataKey(),
                    request.getLeaseToken(),
                    request.getStatusTtl(),
                    request.getDataTtl()
            ));
        }
        return result;
    }

    @Override
    public Map<String, ReadProtocolResult> readProtocolAll(Collection<ReadProtocolRequest> requests) {
        batchReadProtocolCount++;
        Map<String, ReadProtocolResult> result = new LinkedHashMap<String, ReadProtocolResult>();
        for (ReadProtocolRequest request : requests) {
            byte[] data = get(request.getDataKey());
            if (data != null) {
                byte[] validity = get(request.getValidityKey());
                if (validity != null && ProtocolValidity.INVALID.value().equals(new String(validity, java.nio.charset.StandardCharsets.UTF_8))) {
                    result.put(request.getKey(), new ReadProtocolResult(
                            new BatchReadDebugSnapshot(
                                    request.getKey(),
                                    request.getDataKey(),
                                    request.getStatusKey(),
                                    request.getValidityKey(),
                                    true,
                                    null,
                                    ProtocolValidity.INVALID,
                                    BatchReadDebugSnapshot.Decision.INVALID_CACHE
                            ),
                            null));
                } else {
                    result.put(request.getKey(), new ReadProtocolResult(
                            new BatchReadDebugSnapshot(
                                    request.getKey(),
                                    request.getDataKey(),
                                    request.getStatusKey(),
                                    request.getValidityKey(),
                                    true,
                                    null,
                                    validity == null ? null : ProtocolValidity.fromValue(new String(validity, java.nio.charset.StandardCharsets.UTF_8)),
                                    BatchReadDebugSnapshot.Decision.CACHE_HIT
                            ),
                            data));
                }
                continue;
            }
            byte[] status = get(request.getStatusKey());
            if (status == null) {
                result.put(request.getKey(), new ReadProtocolResult(
                        new BatchReadDebugSnapshot(
                                request.getKey(),
                                request.getDataKey(),
                                request.getStatusKey(),
                                request.getValidityKey(),
                                false,
                                null,
                                null,
                                BatchReadDebugSnapshot.Decision.STATUS_MISSING
                        ),
                        null));
                continue;
            }
            String statusValue = new String(status, java.nio.charset.StandardCharsets.UTF_8);
            if (ProtocolState.IN_WRITING.value().equals(statusValue)) {
                result.put(request.getKey(), new ReadProtocolResult(
                        new BatchReadDebugSnapshot(
                                request.getKey(),
                                request.getDataKey(),
                                request.getStatusKey(),
                                request.getValidityKey(),
                                false,
                                ProtocolState.IN_WRITING,
                                null,
                                BatchReadDebugSnapshot.Decision.IN_WRITING
                        ),
                        null));
            } else {
                result.put(request.getKey(), new ReadProtocolResult(
                        new BatchReadDebugSnapshot(
                                request.getKey(),
                                request.getDataKey(),
                                request.getStatusKey(),
                                request.getValidityKey(),
                                false,
                                ProtocolState.LAST_WRITE_SUCCESS,
                                null,
                                BatchReadDebugSnapshot.Decision.LAST_WRITE_SUCCESS
                        ),
                        null));
            }
        }
        return result;
    }

    @Override
    public boolean setIfAbsent(String key, byte[] value, Duration ttl) {
        purgeIfExpired(key);
        return storage.putIfAbsent(key, new Entry(value, expireAt(ttl))) == null;
    }

    @Override
    public boolean compareAndDelete(String key, byte[] expectedValue) {
        purgeIfExpired(key);
        Entry current = storage.get(key);
        if (current == null || !java.util.Arrays.equals(current.value, expectedValue)) {
            return false;
        }
        return storage.remove(key, current);
    }

    @Override
    public boolean beginWrite(String statusKey,
                              String protocolLeaseKey,
                              String dataKey,
                              String validityKey,
                              byte[] leaseToken,
                              Duration writeWindowTtl) {
        purgeIfExpired(statusKey);
        Entry status = storage.get(statusKey);
        if (status != null && ProtocolState.IN_WRITING.value().equals(new String(status.value, java.nio.charset.StandardCharsets.UTF_8))) {
            return false;
        }
        storage.put(statusKey, new Entry(ProtocolState.IN_WRITING.value().getBytes(java.nio.charset.StandardCharsets.UTF_8), expireAt(writeWindowTtl)));
        storage.put(protocolLeaseKey, new Entry(leaseToken, expireAt(writeWindowTtl)));
        storage.remove(dataKey);
        storage.put(validityKey, new Entry(ProtocolValidity.INVALID.value().getBytes(java.nio.charset.StandardCharsets.UTF_8), expireAt(writeWindowTtl)));
        return true;
    }

    @Override
    public boolean stageCacheValue(String protocolLeaseKey,
                                   byte[] expectedValue,
                                   String cacheKey,
                                   byte[] cacheValue,
                                   Duration ttl) {
        purgeIfExpired(protocolLeaseKey);
        Entry lease = storage.get(protocolLeaseKey);
        if (lease == null || !java.util.Arrays.equals(lease.value, expectedValue)) {
            return false;
        }
        storage.put(cacheKey, new Entry(cacheValue, expireAt(ttl)));
        return true;
    }

    @Override
    public boolean writeCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) {
        purgeIfExpired(lockKey);
        Entry current = storage.get(lockKey);
        if (current == null || !java.util.Arrays.equals(current.value, expectedValue)) {
            return false;
        }
        storage.put(cacheKey, new Entry(cacheValue, expireAt(ttl)));
        storage.remove(lockKey, current);
        return true;
    }

    @Override
    public boolean deleteCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey) {
        purgeIfExpired(lockKey);
        Entry current = storage.get(lockKey);
        if (current == null || !java.util.Arrays.equals(current.value, expectedValue)) {
            return false;
        }
        storage.remove(cacheKey);
        storage.remove(lockKey, current);
        return true;
    }

    @Override
    public boolean finalizeWrite(String statusKey,
                                 String protocolLeaseKey,
                                 String validityKey,
                                 String dataKey,
                                 byte[] leaseToken,
                                 Duration statusTtl,
                                 Duration dataTtl) {
        purgeIfExpired(protocolLeaseKey);
        Entry lease = storage.get(protocolLeaseKey);
        if (lease == null || !java.util.Arrays.equals(lease.value, leaseToken)) {
            return false;
        }
        storage.put(statusKey, new Entry(ProtocolState.LAST_WRITE_SUCCESS.value().getBytes(java.nio.charset.StandardCharsets.UTF_8), expireAt(statusTtl)));
        if (storage.containsKey(dataKey)) {
            storage.put(validityKey, new Entry(ProtocolValidity.VALID.value().getBytes(java.nio.charset.StandardCharsets.UTF_8), expireAt(dataTtl)));
        }
        storage.remove(protocolLeaseKey, lease);
        return true;
    }

    @Override
    public boolean rollbackWrite(String statusKey, String protocolLeaseKey, String validityKey, byte[] leaseToken) {
        purgeIfExpired(protocolLeaseKey);
        Entry lease = storage.get(protocolLeaseKey);
        if (lease == null || !java.util.Arrays.equals(lease.value, leaseToken)) {
            return false;
        }
        storage.remove(statusKey);
        storage.remove(protocolLeaseKey, lease);
        storage.remove(validityKey);
        return true;
    }

    @Override
    public long delete(String... keys) {
        long deleted = 0;
        for (String key : keys) {
            if (storage.remove(key) != null) {
                deleted++;
            }
        }
        return deleted;
    }

    @Override
    public void close() {
        storage.clear();
    }

    public boolean contains(String key) {
        return get(key) != null;
    }

    public int getBatchBeginWriteCount() {
        return batchBeginWriteCount;
    }

    public int getBatchStageCacheCount() {
        return batchStageCacheCount;
    }

    public int getBatchFinalizeCount() {
        return batchFinalizeCount;
    }

    public int getBatchReadProtocolCount() {
        return batchReadProtocolCount;
    }

    private void purgeIfExpired(String key) {
        Entry entry = storage.get(key);
        if (entry != null && entry.isExpired()) {
            storage.remove(key);
        }
    }

    private long expireAt(Duration ttl) {
        return System.currentTimeMillis() + ttl.toMillis();
    }

    private static final class Entry {
        private final byte[] value;
        private final long expireAtMillis;

        private Entry(byte[] value, long expireAtMillis) {
            this.value = value;
            this.expireAtMillis = expireAtMillis;
        }

        private boolean isExpired() {
            return System.currentTimeMillis() > expireAtMillis;
        }
    }
}
