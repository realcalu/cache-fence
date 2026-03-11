package io.github.cacheconsistency.core;

import io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import io.github.cacheconsistency.core.support.BatchRedisAccessor;
import io.github.cacheconsistency.core.support.RedisAccessor;
import io.github.cacheconsistency.core.support.RedisProtocolInspector;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RedisProtocolInspectorTest {
    @Test
    void shouldBuildSnapshotFromProtocolKeys() {
        RedisAccessor redisAccessor = mock(RedisAccessor.class);
        when(redisAccessor.get("demo:data:user:1")).thenReturn("value".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(redisAccessor.get("demo:status:user:1")).thenReturn("LAST_WRITE_SUCCESS".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(redisAccessor.get("demo:validity:user:1")).thenReturn("1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(redisAccessor.get("demo:lease:user:1")).thenReturn("read-token".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(redisAccessor.get("demo:protocol-lease:user:1")).thenReturn("write-token".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        ProtocolSnapshot snapshot = new RedisProtocolInspector(redisAccessor, "demo").snapshot("user:1");

        assertEquals("user:1", snapshot.getBusinessKey());
        assertTrue(snapshot.isDataPresent());
        assertEquals(ProtocolState.LAST_WRITE_SUCCESS, snapshot.getState());
        assertEquals(ProtocolValidity.VALID, snapshot.getValidity());
        assertEquals("read-token", snapshot.getReadLeaseToken());
        assertEquals("write-token", snapshot.getWriteLeaseToken());
        assertTrue(snapshot.isCacheReadable());
        assertFalse(snapshot.isGhostWriteSuspected());
    }

    @Test
    void shouldExplainReadDecisionFromProtocolKeys() {
        RedisAccessor redisAccessor = mock(RedisAccessor.class);
        when(redisAccessor.get("demo:data:user:1")).thenReturn("value".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(redisAccessor.get("demo:validity:user:1")).thenReturn("1".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        BatchReadDebugSnapshot snapshot = new RedisProtocolInspector(redisAccessor, "demo").explainRead("user:1");

        assertEquals("user:1", snapshot.getBusinessKey());
        assertEquals("demo:data:user:1", snapshot.getDataKey());
        assertEquals(BatchReadDebugSnapshot.Decision.CACHE_HIT, snapshot.getDecision());
        assertTrue(snapshot.isDataPresent());
        assertEquals(ProtocolValidity.VALID, snapshot.getValidity());
    }

    @Test
    void shouldBuildSnapshotsInBatchWhenAccessorSupportsIt() {
        BatchRedisAccessor redisAccessor = new BatchRedisAccessor() {
            @Override
            public Map<String, byte[]> getAll(java.util.Collection<String> keys) {
                java.util.Map<String, byte[]> result = new java.util.LinkedHashMap<String, byte[]>();
                for (String key : keys) {
                    if (key.contains("data")) {
                        result.put(key, "value".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    } else if (key.contains("status")) {
                        result.put(key, "LAST_WRITE_SUCCESS".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    } else if (key.contains("validity")) {
                        result.put(key, "1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    }
                }
                return result;
            }

            @Override public void setAll(Map<String, byte[]> values, Duration ttl) { throw new UnsupportedOperationException(); }
            @Override public Map<String, Boolean> beginWriteAll(java.util.Collection<BeginWriteRequest> requests) { throw new UnsupportedOperationException(); }
            @Override public Map<String, Boolean> stageCacheValueAll(java.util.Collection<StageCacheValueRequest> requests) { throw new UnsupportedOperationException(); }
            @Override public Map<String, Boolean> finalizeWriteAll(java.util.Collection<FinalizeWriteRequest> requests) { throw new UnsupportedOperationException(); }
            @Override
            public Map<String, ReadProtocolResult> readProtocolAll(java.util.Collection<ReadProtocolRequest> requests) {
                java.util.Map<String, ReadProtocolResult> result = new java.util.LinkedHashMap<String, ReadProtocolResult>();
                for (ReadProtocolRequest request : requests) {
                    result.put(request.getKey(), new ReadProtocolResult(
                            new BatchReadDebugSnapshot(
                                    request.getKey(),
                                    request.getDataKey(),
                                    request.getStatusKey(),
                                    request.getValidityKey(),
                                    true,
                                    null,
                                    ProtocolValidity.VALID,
                                    BatchReadDebugSnapshot.Decision.CACHE_HIT
                            ),
                            "value".getBytes(java.nio.charset.StandardCharsets.UTF_8)
                    ));
                }
                return result;
            }
            @Override public byte[] get(String key) { throw new UnsupportedOperationException(); }
            @Override public void set(String key, byte[] value, Duration ttl) { throw new UnsupportedOperationException(); }
            @Override public boolean setIfAbsent(String key, byte[] value, Duration ttl) { throw new UnsupportedOperationException(); }
            @Override public boolean compareAndDelete(String key, byte[] expectedValue) { throw new UnsupportedOperationException(); }
            @Override public boolean beginWrite(String statusKey, String protocolLeaseKey, String dataKey, String validityKey, byte[] leaseToken, Duration writeWindowTtl) { throw new UnsupportedOperationException(); }
            @Override public boolean stageCacheValue(String protocolLeaseKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) { throw new UnsupportedOperationException(); }
            @Override public boolean writeCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) { throw new UnsupportedOperationException(); }
            @Override public boolean deleteCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey) { throw new UnsupportedOperationException(); }
            @Override public boolean finalizeWrite(String statusKey, String protocolLeaseKey, String validityKey, String dataKey, byte[] leaseToken, Duration statusTtl, Duration dataTtl) { throw new UnsupportedOperationException(); }
            @Override public boolean rollbackWrite(String statusKey, String protocolLeaseKey, String validityKey, byte[] leaseToken) { throw new UnsupportedOperationException(); }
            @Override public long delete(String... keys) { throw new UnsupportedOperationException(); }
            @Override public void close() { }
        };

        Map<String, ProtocolSnapshot> snapshots = new RedisProtocolInspector(redisAccessor, "demo")
                .snapshotAll(Arrays.asList("user:1", "user:2"));

        assertEquals(2, snapshots.size());
        assertTrue(snapshots.get("user:1").isCacheReadable());
        assertTrue(snapshots.get("user:2").isCacheReadable());

        Map<String, BatchReadDebugSnapshot> explainResults = new RedisProtocolInspector(redisAccessor, "demo")
                .explainReadAll(Arrays.asList("user:1", "user:2"));

        assertEquals(BatchReadDebugSnapshot.Decision.CACHE_HIT, explainResults.get("user:1").getDecision());
        assertEquals("demo:data:user:2", explainResults.get("user:2").getDataKey());
    }
}
