package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.BatchDeleteCommand;
import io.github.cacheconsistency.core.BatchSetCommand;
import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.ConsistencySettings;
import io.github.cacheconsistency.core.ReadResult;
import io.github.cacheconsistency.core.StringSerializer;
import io.github.cacheconsistency.core.WriteResult;
import io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.protocol.ProtocolSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import io.github.cacheconsistency.core.support.DefaultConsistencyClient;
import io.github.cacheconsistency.core.support.RedisProtocolInspector;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistencyClientIntegrationTest {
    @Test
    void shouldReadThroughAndBackfillCache() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("itest");
        store.seed("user:1", "alice", 1L);
        redis.set(keys.status("user:1"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), Duration.ofMinutes(5));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").build()
        );

        ReadResult<String> result = client.get("user:1", Duration.ofMinutes(5), ConsistencyContext.create());

        assertEquals("alice", result.getData());
        assertEquals(ReadResult.ReadSource.STORE, result.getSource());
        assertTrue(redis.contains("itest:data:user:1"));
    }

    @Test
    void shouldUpdateStoreAndCacheWithVersionCheck() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("user:2", "before", 7L);

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").build()
        );

        WriteResult result = client.set(
                "user:2",
                "after",
                Duration.ofMinutes(5),
                ConsistencyContext.create().withVersion("7")
        );

        assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, result.getStatus());
        assertEquals("after", store.getValue("user:2"));
        assertEquals("8", store.getVersion("user:2"));
        assertTrue(redis.contains("itest:data:user:2"));
    }

    @Test
    void shouldRejectStaleVersionOnDelete() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("user:3", "alive", 3L);

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").build()
        );

        WriteResult result = client.delete("user:3", ConsistencyContext.create().withVersion("2"));

        assertEquals(WriteResult.WriteStatus.VERSION_REJECTED, result.getStatus());
        assertEquals("alive", store.getValue("user:3"));
    }

    @Test
    void shouldDeleteStoreAndEvictCache() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("user:4", "value", 9L);
        redis.set("itest:data:user:4", StringSerializer.UTF8.serialize("value"), Duration.ofMinutes(5));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").build()
        );

        WriteResult result = client.delete("user:4", ConsistencyContext.create().withVersion("9"));

        assertEquals(WriteResult.WriteStatus.DELETED, result.getStatus());
        assertEquals(null, store.getValue("user:4"));
        assertFalse(redis.contains("itest:data:user:4"));
    }

    @Test
    void shouldReleaseOnlyOwnedLockToken() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        redis.set("itest:lock:user:6", StringSerializer.UTF8.serialize("other-owner"), Duration.ofMinutes(5));

        boolean deleted = redis.compareAndDelete("itest:lock:user:6", StringSerializer.UTF8.serialize("mine"));

        assertFalse(deleted);
        assertTrue(redis.contains("itest:lock:user:6"));
    }

    @Test
    void shouldRejectConcurrentWritersUsingSameVersion() throws Exception {
        final InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        final InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("user:7", "before", 1L);

        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").build()
        );

        List<WriteResult> results = ConcurrentTestHarness.runConcurrently(2, new Callable<WriteResult>() {
            @Override
            public WriteResult call() {
                return client.set("user:7", "after-" + Thread.currentThread().getName(),
                        Duration.ofMinutes(5),
                        ConsistencyContext.create().withVersion("1"));
            }
        });

        int successCount = 0;
        for (WriteResult result : results) {
            if (result.getStatus() == WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED) {
                successCount++;
            }
        }
        assertEquals(1, successCount);
        assertEquals("2", store.getVersion("user:7"));
    }

    @Test
    void shouldReduceReadStampedeWithLease() throws Exception {
        final InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        final InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        final ProtocolKeys keys = new ProtocolKeys("itest");
        store.seed("user:8", "value", 1L);
        store.setReadDelayMillis(80L);
        redis.set(keys.status("user:8"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), Duration.ofMinutes(5));

        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder()
                        .keyPrefix("itest")
                        .retryBackoffMillis(100)
                        .build()
        );

        List<ReadResult<String>> results = ConcurrentTestHarness.runConcurrently(8, new Callable<ReadResult<String>>() {
            @Override
            public ReadResult<String> call() {
                return client.get("user:8", Duration.ofMinutes(5), ConsistencyContext.create());
            }
        });

        for (ReadResult<String> result : results) {
            assertEquals("value", result.getData());
        }
        assertTrue(store.getReadCount() < 8);
        assertTrue(redis.contains("itest:data:user:8"));
    }

    @Test
    void shouldTreatMissingStatusAsGhostWriteAndAvoidBackfill() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("ghost:1", "db-value", 4L);

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        ReadResult<String> result = client.get("ghost:1", Duration.ofMinutes(5), ConsistencyContext.create());

        assertEquals(ReadResult.ReadSource.STORE, result.getSource());
        assertFalse(redis.contains("itest:data:ghost:1"));
    }

    @Test
    void shouldHealGhostWriteByBumpingVersion() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("ghost:2", "db-value", 4L);
        InMemoryGhostWriteHealer healer = new InMemoryGhostWriteHealer(store);

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").ghostWriteHealingEnabled(true).build(),
                new InMemoryConsistencyObserver(),
                healer
        );

        client.get("ghost:2", Duration.ofMinutes(5), ConsistencyContext.create());
        Thread.sleep(120L);

        assertEquals("5", store.getVersion("ghost:2"));
        assertFalse(redis.contains("itest:data:ghost:2"));
    }

    @Test
    void shouldSupportBatchReadWriteDelete() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("batch:1", "a");
        values.put("batch:2", "b");

        Map<String, WriteResult> setResults = client.setAll(values, Duration.ofMinutes(5), ConsistencyContext.create());
        Map<String, ReadResult<String>> readResults = client.getAll(Arrays.asList("batch:1", "batch:2"),
                Duration.ofMinutes(5), ConsistencyContext.create());
        Map<String, WriteResult> deleteResults = client.deleteAll(Arrays.asList("batch:1", "batch:2"),
                ConsistencyContext.create());

        assertEquals(2, setResults.size());
        assertEquals("a", readResults.get("batch:1").getData());
        assertEquals(2, deleteResults.size());
        assertFalse(redis.contains("itest:data:batch:1"));
        assertEquals(1, store.getBatchUpdateCount());
        assertEquals(1, store.getBatchDeleteCount());
        assertEquals(2, redis.getBatchBeginWriteCount());
        assertEquals(1, redis.getBatchStageCacheCount());
        assertEquals(2, redis.getBatchFinalizeCount());
    }

    @Test
    void shouldUseBatchStoreReadForBatchCacheMiss() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("itest");
        store.seed("batch:r1", "a", 1L);
        store.seed("batch:r2", "b", 1L);
        redis.set(keys.status("batch:r1"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), Duration.ofMinutes(5));
        redis.set(keys.status("batch:r2"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), Duration.ofMinutes(5));
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        Map<String, ReadResult<String>> readResults = client.getAll(Arrays.asList("batch:r1", "batch:r2"),
                Duration.ofMinutes(5), ConsistencyContext.create());

        assertEquals("a", readResults.get("batch:r1").getData());
        assertEquals("b", readResults.get("batch:r2").getData());
        assertEquals(1, store.getBatchReadCount());
        assertEquals(1, redis.getBatchReadProtocolCount());
    }

    @Test
    void shouldUseBatchVersionQueryDuringBatchWrite() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("batch:v1", "a", 1L);
        store.seed("batch:v2", "b", 2L);
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("batch:v1", "next-a");
        values.put("batch:v2", "next-b");
        Map<String, WriteResult> setResults = client.setAll(values, Duration.ofMinutes(5),
                ConsistencyContext.create().withVersion("1"));

        assertEquals(2, setResults.size());
        assertEquals(1, store.getBatchQueryVersionCount());
    }

    @Test
    void shouldSupportPerItemContextInBatchWriteAndDelete() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("batch:c1", "a", 1L);
        store.seed("batch:c2", "b", 2L);
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        List<BatchSetCommand<String>> setCommands = Arrays.asList(
                BatchSetCommand.of("batch:c1", "next-a", ConsistencyContext.create().withVersion("1")),
                BatchSetCommand.of("batch:c2", "next-b", ConsistencyContext.create().withVersion("2"))
        );

        Map<String, WriteResult> setResults = client.setAllWithContexts(
                setCommands,
                Duration.ofMinutes(5),
                ConsistencyContext.create().putAttachment("requestId", "batch-1")
        );

        assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, setResults.get("batch:c1").getStatus());
        assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, setResults.get("batch:c2").getStatus());
        assertEquals("2", store.getVersion("batch:c1"));
        assertEquals("3", store.getVersion("batch:c2"));

        List<BatchDeleteCommand> deleteCommands = Arrays.asList(
                BatchDeleteCommand.of("batch:c1", ConsistencyContext.create().withVersion("2")),
                BatchDeleteCommand.of("batch:c2", ConsistencyContext.create().withVersion("3"))
        );

        Map<String, WriteResult> deleteResults = client.deleteAllWithContexts(
                deleteCommands,
                ConsistencyContext.create().putAttachment("requestId", "batch-2")
        );

        assertEquals(WriteResult.WriteStatus.DELETED, deleteResults.get("batch:c1").getStatus());
        assertEquals(WriteResult.WriteStatus.DELETED, deleteResults.get("batch:c2").getStatus());
        assertEquals(null, store.getValue("batch:c1"));
        assertEquals(null, store.getValue("batch:c2"));
    }

    @Test
    void shouldExposeDetailedBatchWriteSteps() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("batch:d1", "a", 1L);
        store.seed("batch:d2", "b", 2L);
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        List<BatchSetCommand<String>> setCommands = Arrays.asList(
                BatchSetCommand.of("batch:d1", "next-a", ConsistencyContext.create().withVersion("1")),
                BatchSetCommand.of("batch:d2", "next-b", ConsistencyContext.create().withVersion("9"))
        );

        Map<String, BatchWriteDebugSnapshot> setResults = client.setAllWithContextsDetailed(
                setCommands,
                Duration.ofMinutes(5),
                ConsistencyContext.create()
        );

        assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, setResults.get("batch:d1").getFinalStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.SUCCESS, setResults.get("batch:d1").getPrepareStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.SUCCESS, setResults.get("batch:d1").getVersionStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.SUCCESS, setResults.get("batch:d1").getStoreStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.SUCCESS, setResults.get("batch:d1").getStageStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.SUCCESS, setResults.get("batch:d1").getFinalizeStatus());

        assertEquals(WriteResult.WriteStatus.VERSION_REJECTED, setResults.get("batch:d2").getFinalStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.REJECTED, setResults.get("batch:d2").getVersionStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.NOT_RUN, setResults.get("batch:d2").getStoreStatus());

        List<BatchDeleteCommand> deleteCommands = Arrays.asList(
                BatchDeleteCommand.of("batch:d1", ConsistencyContext.create().withVersion("2"))
        );
        Map<String, BatchWriteDebugSnapshot> deleteResults = client.deleteAllWithContextsDetailed(
                deleteCommands,
                ConsistencyContext.create()
        );

        assertEquals(WriteResult.WriteStatus.DELETED, deleteResults.get("batch:d1").getFinalStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.SKIPPED, deleteResults.get("batch:d1").getStageStatus());
        assertEquals(BatchWriteDebugSnapshot.StepStatus.SUCCESS, deleteResults.get("batch:d1").getFinalizeStatus());
    }

    @Test
    void shouldInvalidateStaleCacheDuringWriteWindow() {
        final InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        final InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        final ProtocolKeys keys = new ProtocolKeys("itest");
        store.seed("user:9", "db-new", 1L);
        redis.set(keys.data("user:9"), StringSerializer.UTF8.serialize("stale"), Duration.ofMinutes(5));

        store.setBeforeUpdateHook(new Runnable() {
            @Override
            public void run() {
                assertFalse(redis.contains(keys.data("user:9")));
                byte[] validity = redis.get(keys.validity("user:9"));
                assertEquals(ProtocolValidity.INVALID.value(), StringSerializer.UTF8.deserialize(validity));
            }
        });

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        WriteResult result = client.set("user:9", "db-new", Duration.ofMinutes(5), ConsistencyContext.create().withVersion("1"));

        assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, result.getStatus());
        assertEquals("db-new", StringSerializer.UTF8.deserialize(redis.get(keys.data("user:9"))));
        assertEquals(ProtocolValidity.VALID.value(), StringSerializer.UTF8.deserialize(redis.get(keys.validity("user:9"))));
    }

    @Test
    void shouldExposeProtocolSnapshotWithoutManualKeyAssembly() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        ProtocolKeys keys = new ProtocolKeys("itest");
        redis.set(keys.data("user:10"), StringSerializer.UTF8.serialize("cached"), Duration.ofMinutes(5));
        redis.set(keys.status("user:10"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), Duration.ofMinutes(5));
        redis.set(keys.validity("user:10"), StringSerializer.UTF8.serialize(ProtocolValidity.VALID.value()), Duration.ofMinutes(5));
        redis.set(keys.lease("user:10"), StringSerializer.UTF8.serialize("read-lease"), Duration.ofMinutes(5));

        ProtocolSnapshot snapshot = new RedisProtocolInspector(redis, "itest").snapshot("user:10");

        assertTrue(snapshot.isDataPresent());
        assertEquals(ProtocolState.LAST_WRITE_SUCCESS, snapshot.getState());
        assertEquals(ProtocolValidity.VALID, snapshot.getValidity());
        assertEquals("read-lease", snapshot.getReadLeaseToken());
        assertTrue(snapshot.isCacheReadable());
        assertFalse(snapshot.isGhostWriteSuspected());
    }

    @Test
    void shouldExposeGhostWriteDiagnosis() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        io.github.cacheconsistency.core.support.ProtocolDiagnostician diagnostician =
                new io.github.cacheconsistency.core.support.DefaultProtocolDiagnostician(
                        new RedisProtocolInspector(redis, "itest"));

        io.github.cacheconsistency.core.protocol.ProtocolDiagnosis diagnosis = diagnostician.diagnose("ghost:diag");

        assertTrue(diagnosis.getSummary().contains("ghost write"));
        assertTrue(diagnosis.format().contains("ghost write"));
    }

    @Test
    void shouldFallbackToStoreWhenProtocolWindowExpires() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("itest");
        store.seed("user:11", "value", 1L);
        redis.set(keys.status("user:11"), StringSerializer.UTF8.serialize(ProtocolState.IN_WRITING.value()), Duration.ofMillis(80));
        redis.set(keys.validity("user:11"), StringSerializer.UTF8.serialize(ProtocolValidity.INVALID.value()), Duration.ofMillis(80));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        ReadResult<String> duringWindow = client.get("user:11", Duration.ofMinutes(5), ConsistencyContext.create());
        Thread.sleep(120L);
        ReadResult<String> afterExpiry = client.get("user:11", Duration.ofMinutes(5), ConsistencyContext.create());

        assertEquals(ReadResult.ReadSource.STORE, duringWindow.getSource());
        assertEquals("value", afterExpiry.getData());
        assertFalse(redis.contains(keys.data("user:11")));
    }

    @Test
    void shouldHandleHeavierConcurrentReadPressure() throws Exception {
        final InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        final InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        final ProtocolKeys keys = new ProtocolKeys("itest");
        store.seed("user:12", "value", 1L);
        store.setReadDelayMillis(120L);
        redis.set(keys.status("user:12"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), Duration.ofMinutes(5));

        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").retryBackoffMillis(150).build()
        );

        List<ReadResult<String>> results = ConcurrentTestHarness.runConcurrently(24, new Callable<ReadResult<String>>() {
            @Override
            public ReadResult<String> call() {
                return client.get("user:12", Duration.ofMinutes(5), ConsistencyContext.create());
            }
        });

        assertEquals(24, results.size());
        assertTrue(store.getReadCount() < 10);
        assertTrue(redis.contains(keys.data("user:12")));
    }

    @Test
    void shouldSurfaceStoreReadFailureForSlowOrBrokenQuery() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.setReadFailure(new RuntimeException("db timeout"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("itest").build());

        RuntimeException error = null;
        try {
            client.get("user:13", Duration.ofMinutes(5), ConsistencyContext.create());
        } catch (RuntimeException ex) {
            error = ex;
        }

        assertNotNull(error);
        assertTrue(error.getMessage().contains("Persistent store read failed") || error.getMessage().contains("db timeout"));
    }

    @Test
    void shouldRejectLateWriterAfterGhostHealingWinsRace() throws Exception {
        final InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        final InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("ghost:race", "before", 4L);
        final InMemoryGhostWriteHealer healer = new InMemoryGhostWriteHealer(store);
        healer.setDelayMillis(10L);

        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("itest").ghostWriteHealingEnabled(true).build(),
                new InMemoryConsistencyObserver(),
                healer
        );

        final AtomicReference<WriteResult> writeResultRef = new AtomicReference<WriteResult>();
        Thread healerTrigger = new Thread(new Runnable() {
            @Override
            public void run() {
                client.get("ghost:race", Duration.ofMinutes(5), ConsistencyContext.create());
            }
        });
        Thread writer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(30L);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
                writeResultRef.set(client.set(
                        "ghost:race",
                        "after",
                        Duration.ofMinutes(5),
                        ConsistencyContext.create().withVersion("4")
                ));
            }
        });

        healerTrigger.start();
        writer.start();
        healerTrigger.join();
        writer.join();
        Thread.sleep(120L);

        WriteResult writeResult = writeResultRef.get();
        assertNotNull(writeResult);
        assertEquals(WriteResult.WriteStatus.VERSION_REJECTED, writeResult.getStatus());
        assertEquals("5", store.getVersion("ghost:race"));
    }
}
