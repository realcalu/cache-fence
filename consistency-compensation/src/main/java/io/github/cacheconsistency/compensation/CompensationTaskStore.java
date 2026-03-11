package io.github.cacheconsistency.compensation;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Persistence abstraction for optional compensation tasks.
 */
public interface CompensationTaskStore {
    CompensationTask save(CompensationTask task);

    List<CompensationTask> loadPending();

    void markSuccess(String taskId);

    void markFailure(String taskId, int attempt, String message, boolean terminal);

    final class NoOpCompensationTaskStore implements CompensationTaskStore {
        public static final NoOpCompensationTaskStore INSTANCE = new NoOpCompensationTaskStore();

        private NoOpCompensationTaskStore() {
        }

        @Override
        public CompensationTask save(CompensationTask task) {
            return task;
        }

        @Override
        public List<CompensationTask> loadPending() {
            return Collections.emptyList();
        }

        @Override
        public void markSuccess(String taskId) {
        }

        @Override
        public void markFailure(String taskId, int attempt, String message, boolean terminal) {
        }
    }

    final class CompensationTask {
        public enum Action {
            WRITE,
            DELETE
        }

        private final String id;
        private final Action action;
        private final String cacheKey;
        private final byte[] payload;
        private final long ttlSeconds;
        private final int attempt;

        CompensationTask(String id, Action action, String cacheKey, byte[] payload, long ttlSeconds, int attempt) {
            this.id = id;
            this.action = action;
            this.cacheKey = cacheKey;
            this.payload = payload;
            this.ttlSeconds = ttlSeconds;
            this.attempt = attempt;
        }

        public static CompensationTask write(String cacheKey, byte[] payload, Duration ttl) {
            return new CompensationTask(UUID.randomUUID().toString(), Action.WRITE, cacheKey, payload, ttl.getSeconds(), 0);
        }

        public static CompensationTask delete(String cacheKey) {
            return new CompensationTask(UUID.randomUUID().toString(), Action.DELETE, cacheKey, null, 0L, 0);
        }

        public CompensationTask withAttempt(int attempt) {
            return new CompensationTask(id, action, cacheKey, payload, ttlSeconds, attempt);
        }

        public String getId() {
            return id;
        }

        public Action getAction() {
            return action;
        }

        public String getCacheKey() {
            return cacheKey;
        }

        public byte[] getPayload() {
            return payload;
        }

        public long getTtlSeconds() {
            return ttlSeconds;
        }

        public int getAttempt() {
            return attempt;
        }
    }
}
