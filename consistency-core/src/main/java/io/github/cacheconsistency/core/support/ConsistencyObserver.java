package io.github.cacheconsistency.core.support;

public interface ConsistencyObserver {
    void onCacheHit(String key);

    void onCacheMiss(String key);

    void onLeaseAcquired(String key);

    void onStoreRead(String key);

    void onStoreWrite(String key);

    void onStoreDelete(String key);

    void onVersionRejected(String key);

    void onFinalizeFailure(String key, String action, Throwable error);

    void onGhostWriteHealScheduled(String key);

    void onGhostWriteHealSuccess(String key);

    void onGhostWriteHealFailure(String key, Throwable error);

    void onBatchOperation(String action, int size, boolean optimized);

    void onCompensationScheduled(String key, String action);

    void onCompensationSuccess(String key, String action, int attempt);

    void onCompensationFailure(String key, String action, int attempt, Throwable error);

    final class NoOpConsistencyObserver implements ConsistencyObserver {
        public static final NoOpConsistencyObserver INSTANCE = new NoOpConsistencyObserver();

        private NoOpConsistencyObserver() {
        }

        @Override
        public void onCacheHit(String key) {
        }

        @Override
        public void onCacheMiss(String key) {
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
        }

        @Override
        public void onCompensationSuccess(String key, String action, int attempt) {
        }

        @Override
        public void onCompensationFailure(String key, String action, int attempt, Throwable error) {
        }
    }
}
