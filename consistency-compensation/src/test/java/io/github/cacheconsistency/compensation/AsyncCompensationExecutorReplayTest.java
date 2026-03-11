package io.github.cacheconsistency.compensation;

import io.github.cacheconsistency.core.support.RedisAccessor;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncCompensationExecutorReplayTest {
    @Test
    void shouldReplayPendingWriteTaskFromStore() throws Exception {
        Path file = Files.createTempFile("cck-compensation-replay", ".log");
        try {
            FileCompensationTaskStore store = new FileCompensationTaskStore(file);
            store.save(CompensationTaskStore.CompensationTask.write(
                    "cache:key", "value".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(10)));
            RecordingRedisAccessor redisAccessor = new RecordingRedisAccessor();
            AsyncCompensationExecutor executor = new AsyncCompensationExecutor(
                    redisAccessor, 1, 1L, store, io.github.cacheconsistency.core.support.ConsistencyObserver.NoOpConsistencyObserver.INSTANCE);

            executor.replayPending();
            Thread.sleep(50L);
            executor.shutdown();

            assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), redisAccessor.values.get("cache:key"));
            assertTrue(store.loadPending().isEmpty());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void shouldReplayPendingDeleteTaskFromStore() throws Exception {
        Path file = Files.createTempFile("cck-compensation-replay", ".log");
        try {
            FileCompensationTaskStore store = new FileCompensationTaskStore(file);
            store.save(CompensationTaskStore.CompensationTask.delete("cache:key"));
            RecordingRedisAccessor redisAccessor = new RecordingRedisAccessor();
            redisAccessor.values.put("cache:key", "value".getBytes(StandardCharsets.UTF_8));
            AsyncCompensationExecutor executor = new AsyncCompensationExecutor(
                    redisAccessor, 1, 1L, store, io.github.cacheconsistency.core.support.ConsistencyObserver.NoOpConsistencyObserver.INSTANCE);

            executor.replayPending();
            Thread.sleep(50L);
            executor.shutdown();

            assertNull(redisAccessor.values.get("cache:key"));
            assertTrue(store.loadPending().isEmpty());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static final class RecordingRedisAccessor implements RedisAccessor {
        private final ConcurrentMap<String, byte[]> values = new ConcurrentHashMap<String, byte[]>();

        @Override
        public byte[] get(String key) {
            return values.get(key);
        }

        @Override
        public void set(String key, byte[] value, Duration ttl) {
            values.put(key, value);
        }

        @Override
        public boolean setIfAbsent(String key, byte[] value, Duration ttl) {
            return values.putIfAbsent(key, value) == null;
        }

        @Override
        public boolean compareAndDelete(String key, byte[] expectedValue) {
            byte[] current = values.get(key);
            if (current == null) {
                return false;
            }
            if (!java.util.Arrays.equals(current, expectedValue)) {
                return false;
            }
            values.remove(key);
            return true;
        }

        @Override
        public boolean beginWrite(String statusKey, String protocolLeaseKey, String dataKey, String validityKey, byte[] leaseToken, Duration writeWindowTtl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stageCacheValue(String protocolLeaseKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean writeCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean deleteCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean finalizeWrite(String statusKey, String protocolLeaseKey, String validityKey, String dataKey, byte[] leaseToken, Duration statusTtl, Duration dataTtl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rollbackWrite(String statusKey, String protocolLeaseKey, String validityKey, byte[] leaseToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long delete(String... keys) {
            long deleted = 0L;
            for (String key : keys) {
                if (values.remove(key) != null) {
                    deleted++;
                }
            }
            return deleted;
        }

        @Override
        public void close() {
        }
    }
}
