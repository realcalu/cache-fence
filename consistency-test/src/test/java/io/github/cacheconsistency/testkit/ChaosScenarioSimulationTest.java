package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.ConsistencySettings;
import io.github.cacheconsistency.core.ReadResult;
import io.github.cacheconsistency.core.StringSerializer;
import io.github.cacheconsistency.core.WriteResult;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import io.github.cacheconsistency.core.support.DefaultConsistencyClient;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChaosScenarioSimulationTest {
    @Test
    void scenario1ClientTimeoutShouldNotBreakReadConsistency() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("scenario:1", "before", 1L);
        store.setUpdateDelayMillis(200L);

        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("chaos").build());

        ExecutorService executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        try {
            Future<WriteResult> future = executor.submit(new Callable<WriteResult>() {
                @Override
                public WriteResult call() {
                    return client.set("scenario:1", "after", Duration.ofMinutes(5),
                            ConsistencyContext.create().withVersion("1"));
                }
            });

            boolean timedOut = false;
            try {
                future.get(50L, TimeUnit.MILLISECONDS);
            } catch (TimeoutException expected) {
                timedOut = true;
            }

            WriteResult finalResult = future.get(1L, TimeUnit.SECONDS);
            ReadResult<String> readResult = client.get("scenario:1", Duration.ofMinutes(5), ConsistencyContext.create());

            assertTrue(timedOut);
            assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, finalResult.getStatus());
            assertEquals("after", readResult.getData());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void scenario2BlockedStepShouldStillAllowConsistentStoreReads() throws Exception {
        final InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        final InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("scenario:2", "value", 1L);
        store.setBeforeUpdateHook(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(150L);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("chaos").build());

        ExecutorService executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        try {
            Future<WriteResult> future = executor.submit(new Callable<WriteResult>() {
                @Override
                public WriteResult call() {
                    return client.set("scenario:2", "new-value", Duration.ofMinutes(5),
                            ConsistencyContext.create().withVersion("1"));
                }
            });

            Thread.sleep(30L);
            ReadResult<String> readDuringBlock = client.get("scenario:2", Duration.ofMinutes(5), ConsistencyContext.create());

            assertEquals(ReadResult.ReadSource.STORE, readDuringBlock.getSource());
            assertEquals("value", readDuringBlock.getData());
            assertEquals(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED, future.get(1L, TimeUnit.SECONDS).getStatus());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void scenario3DatabaseFailureShouldNotBreakReadConsistency() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("scenario:3", "stable", 5L);
        store.setQueryVersionFailure(new RuntimeException("version query timeout"));

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("chaos").build());

        RuntimeException writeError = null;
        try {
            client.set("scenario:3", "broken", Duration.ofMinutes(5), ConsistencyContext.create().withVersion("5"));
        } catch (RuntimeException error) {
            writeError = error;
        }

        ReadResult<String> readResult = client.get("scenario:3", Duration.ofMinutes(5), ConsistencyContext.create());

        assertNotNull(writeError);
        assertEquals("stable", readResult.getData());
        assertEquals(ReadResult.ReadSource.STORE, readResult.getSource());
    }

    @Test
    void scenario4ShortLeaseShouldIncreaseContentionWithoutBreakingConsistency() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("chaos");
        store.seed("scenario:4", "lease-value", 1L);
        store.setReadDelayMillis(1200L);
        redis.set(keys.status("scenario:4"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()),
                Duration.ofMinutes(5));

        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder()
                        .keyPrefix("chaos")
                        .leaseTtlSeconds(1)
                        .retryBackoffMillis(20)
                        .build()
        );

        List<ReadResult<String>> results = ConcurrentTestHarness.runConcurrently(6, new Callable<ReadResult<String>>() {
            @Override
            public ReadResult<String> call() {
                return client.get("scenario:4", Duration.ofMinutes(5), ConsistencyContext.create());
            }
        });

        for (ReadResult<String> result : results) {
            assertEquals("lease-value", result.getData());
        }
        assertTrue(store.getReadCount() >= 1);
    }

    @Test
    void scenario5RedisFailoverShouldFallbackToStoreAndRecover() {
        InMemoryRedisAccessor baseRedis = new InMemoryRedisAccessor();
        FaultInjectingRedisAccessor redis = new FaultInjectingRedisAccessor(baseRedis);
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("chaos");
        store.seed("scenario:5", "db-value", 1L);
        baseRedis.set(keys.status("scenario:5"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()),
                Duration.ofMinutes(5));
        redis.failNextGets(1);

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("chaos").build());

        ReadResult<String> duringFailover = client.get("scenario:5", Duration.ofMinutes(5), ConsistencyContext.create());
        ReadResult<String> afterRecovery = client.get("scenario:5", Duration.ofMinutes(5), ConsistencyContext.create());

        assertEquals("db-value", duringFailover.getData());
        assertEquals("db-value", afterRecovery.getData());
    }

    @Test
    void scenario6MetadataLossShouldFallbackToStoreAndKeepConsistency() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("chaos");
        store.seed("scenario:6", "db-value", 9L);
        InMemoryGhostWriteHealer healer = new InMemoryGhostWriteHealer(store);

        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("chaos").ghostWriteHealingEnabled(true).build(),
                new InMemoryConsistencyObserver(),
                healer
        );

        redis.set(keys.status("scenario:6"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()),
                Duration.ofMinutes(5));
        redis.set(keys.validity("scenario:6"), StringSerializer.UTF8.serialize(ProtocolValidity.VALID.value()),
                Duration.ofMinutes(5));
        redis.delete(keys.status("scenario:6"), keys.protocolLease("scenario:6"), keys.data("scenario:6"));

        ReadResult<String> result = client.get("scenario:6", Duration.ofMinutes(5), ConsistencyContext.create());
        Thread.sleep(120L);

        assertEquals(ReadResult.ReadSource.STORE, result.getSource());
        assertEquals("db-value", result.getData());
        assertFalse(redis.contains(keys.data("scenario:6")));
        assertEquals("10", store.getVersion("scenario:6"));
    }
}
