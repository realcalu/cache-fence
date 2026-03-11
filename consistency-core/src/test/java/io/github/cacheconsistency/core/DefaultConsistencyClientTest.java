package io.github.cacheconsistency.core;

import io.github.cacheconsistency.core.support.DefaultConsistencyClient;
import io.github.cacheconsistency.core.support.ConsistencyObserver;
import io.github.cacheconsistency.core.support.FinalizeFailureHandler;
import io.github.cacheconsistency.core.support.GhostWriteHealer;
import io.github.cacheconsistency.core.support.RedisAccessor;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultConsistencyClientTest {
    @Test
    void shouldReturnCacheValueWhenHit() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(redis.get("test:data:order:1")).thenReturn("cached".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        ReadResult<String> result = client.get("order:1", Duration.ofSeconds(30), ConsistencyContext.create());
        assertEquals("cached", result.getData());
        assertEquals(ReadResult.ReadSource.CACHE, result.getSource());
    }

    @Test
    void shouldReadStoreAndPopulateCacheWhenLeaseAcquired() {
        RedisAccessor redis = mock(RedisAccessor.class);
        ProtocolKeys keys = new ProtocolKeys("test");
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(redis.get(keys.status("order:2"))).thenReturn(ProtocolState.LAST_WRITE_SUCCESS.value().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        when(redis.setIfAbsent(anyString(), any(byte[].class), any(Duration.class))).thenReturn(true);
        when(store.read(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success("db-value"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        ReadResult<String> result = client.get("order:2", Duration.ofSeconds(30), ConsistencyContext.create());
        assertEquals("db-value", result.getData());
        assertEquals(ReadResult.ReadSource.STORE, result.getSource());
        verify(redis).set(keys.data("order:2"), "db-value".getBytes(java.nio.charset.StandardCharsets.UTF_8), Duration.ofSeconds(30));
        verify(redis).compareAndDelete(anyString(), any(byte[].class));
    }

    @Test
    void shouldWriteStoreAndCache() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(store.update(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success());
        when(redis.stageCacheValue(anyString(), any(byte[].class), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        WriteResult result = client.set("order:3", "new-value", Duration.ofSeconds(60), ConsistencyContext.create());
        assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, result.getStatus());
        verify(redis).stageCacheValue(anyString(), any(byte[].class), anyString(), any(byte[].class), any(Duration.class));
        verify(redis, times(1)).compareAndDelete(anyString(), any(byte[].class));
    }

    @Test
    void shouldRejectVersionBeforeStoreUpdate() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(store.queryVersion(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success("9"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        WriteResult result = client.set(
                "order:4",
                "new-value",
                Duration.ofSeconds(60),
                ConsistencyContext.create().withVersion("8")
        );

        assertEquals(WriteResult.WriteStatus.VERSION_REJECTED, result.getStatus());
        verify(store, never()).update(any(ConsistencyContext.class));
        verify(redis).rollbackWrite(anyString(), anyString(), anyString(), any(byte[].class));
    }

    @Test
    void shouldFailWhenProtocolWriteStageFailsAfterStoreUpdate() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(store.update(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success());
        when(redis.stageCacheValue(anyString(), any(byte[].class), anyString(), any(byte[].class), any(Duration.class)))
                .thenThrow(new RuntimeException("boom"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        assertThrows(IllegalStateException.class,
                () -> client.set("order:5", "new-value", Duration.ofSeconds(60), ConsistencyContext.create()));
    }

    @Test
    void shouldFailByDefaultWhenFinalizeFailsAfterStoreUpdate() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(store.update(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success());
        when(redis.stageCacheValue(anyString(), any(byte[].class), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(redis.finalizeWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class), any(Duration.class)))
                .thenThrow(new RuntimeException("finalize-boom"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        assertThrows(IllegalStateException.class,
                () -> client.set("order:5b", "new-value", Duration.ofSeconds(60), ConsistencyContext.create()));
    }

    @Test
    void shouldUseOptionalFinalizeFailureHandlerForSet() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        FinalizeFailureHandler finalizeFailureHandler = mock(FinalizeFailureHandler.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(store.update(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success());
        when(redis.stageCacheValue(anyString(), any(byte[].class), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(redis.finalizeWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class), any(Duration.class)))
                .thenThrow(new RuntimeException("finalize-boom"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build(),
                ConsistencyObserver.NoOpConsistencyObserver.INSTANCE,
                GhostWriteHealer.NoOpGhostWriteHealer.INSTANCE,
                finalizeFailureHandler
        );

        WriteResult result = client.set("order:5c", "new-value", Duration.ofSeconds(60), ConsistencyContext.create());

        assertEquals(WriteResult.WriteStatus.STORE_UPDATED_CACHE_REPAIR_SCHEDULED, result.getStatus());
        verify(finalizeFailureHandler).onSetFinalizeFailure(
                anyString(), anyString(), any(byte[].class), any(Duration.class), any(ConsistencyContext.class), any(RuntimeException.class));
    }

    @Test
    void shouldUseOptionalFinalizeFailureHandlerForDelete() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        FinalizeFailureHandler finalizeFailureHandler = mock(FinalizeFailureHandler.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(store.delete(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success());
        when(redis.finalizeWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class), any(Duration.class)))
                .thenThrow(new RuntimeException("finalize-boom"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build(),
                ConsistencyObserver.NoOpConsistencyObserver.INSTANCE,
                GhostWriteHealer.NoOpGhostWriteHealer.INSTANCE,
                finalizeFailureHandler
        );

        WriteResult result = client.delete("order:5d", ConsistencyContext.create());

        assertEquals(WriteResult.WriteStatus.DELETED_CACHE_REPAIR_SCHEDULED, result.getStatus());
        verify(finalizeFailureHandler).onDeleteFinalizeFailure(
                anyString(), anyString(), any(ConsistencyContext.class), any(RuntimeException.class));
    }

    @Test
    void shouldRejectBlankKey() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        assertThrows(IllegalArgumentException.class,
                () -> client.get(" ", Duration.ofSeconds(30), ConsistencyContext.create()));
    }

    @Test
    void shouldSupportBatchOperations() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(redis.stageCacheValue(anyString(), any(byte[].class), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(store.update(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success());
        when(store.read(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success("value"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("test").build());

        Map<String, String> values = new HashMap<String, String>();
        values.put("a", "1");
        values.put("b", "2");
        Map<String, WriteResult> writeResults = client.setAll(values, Duration.ofSeconds(30), ConsistencyContext.create());
        Map<String, ReadResult<String>> readResults = client.getAll(Arrays.asList("a", "b"), Duration.ofSeconds(30), ConsistencyContext.create());

        assertEquals(2, writeResults.size());
        assertEquals(2, readResults.size());
    }

    @Test
    void shouldExecuteBatchWritesInParallelWhenConfigured() {
        RedisAccessor redis = mock(RedisAccessor.class);
        when(redis.beginWrite(anyString(), anyString(), anyString(), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        when(redis.stageCacheValue(anyString(), any(byte[].class), anyString(), any(byte[].class), any(Duration.class)))
                .thenReturn(true);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        when(store.update(any(ConsistencyContext.class))).thenAnswer(invocation -> {
            int current = active.incrementAndGet();
            maxActive.updateAndGet(previous -> Math.max(previous, current));
            try {
                Thread.sleep(80L);
            } finally {
                active.decrementAndGet();
            }
            return PersistentOperation.OperationResult.success();
        });

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").batchParallelism(2).build());

        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("a", "1");
        values.put("b", "2");
        client.setAll(values, Duration.ofSeconds(30), ConsistencyContext.create());

        assertEquals(2, maxActive.get());
    }

    @Test
    void shouldNotifyObserver() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        ConsistencyObserver observer = mock(ConsistencyObserver.class);
        when(redis.get("test:data:order:7")).thenReturn("cached".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build(),
                observer
        );

        client.get("order:7", Duration.ofSeconds(30), ConsistencyContext.create());
        verify(observer).onCacheHit("order:7");
    }

    @Test
    void shouldReadStoreWithoutBackfillWhenStatusMissing() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        when(store.read(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success("db-value"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").build()
        );

        ReadResult<String> result = client.get("order:8", Duration.ofSeconds(30), ConsistencyContext.create());

        assertEquals(ReadResult.ReadSource.STORE, result.getSource());
        verify(redis, never()).setIfAbsent(anyString(), any(byte[].class), any(Duration.class));
    }

    @Test
    void shouldTriggerGhostHealWhenStatusMissingAndSwitchEnabled() {
        RedisAccessor redis = mock(RedisAccessor.class);
        @SuppressWarnings("unchecked")
        PersistentOperation<String> store = mock(PersistentOperation.class);
        GhostWriteHealer healer = mock(GhostWriteHealer.class);
        when(store.read(any(ConsistencyContext.class))).thenReturn(PersistentOperation.OperationResult.success("db-value"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("test").ghostWriteHealingEnabled(true).build(),
                ConsistencyObserver.NoOpConsistencyObserver.INSTANCE,
                healer
        );

        client.get("order:9", Duration.ofSeconds(30), ConsistencyContext.create());

        verify(healer).scheduleHeal(anyString(), any(ConsistencyContext.class));
    }
}
