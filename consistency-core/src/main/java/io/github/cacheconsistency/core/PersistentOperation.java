package io.github.cacheconsistency.core;

/**
 * Business-owned adapter for the persistent store behind the consistency protocol.
 *
 * <p>Implementations are responsible for reading business data, querying the current
 * version and enforcing version checks during writes.</p>
 */
public interface PersistentOperation<T> {
    /**
     * Updates the store with the value carried in the context.
     */
    OperationResult<Void> update(ConsistencyContext context);

    /**
     * Deletes the value represented by the context.
     */
    OperationResult<Void> delete(ConsistencyContext context);

    /**
     * Returns the current store version used by the protocol to reject stale writers.
     */
    OperationResult<String> queryVersion(ConsistencyContext context);

    /**
     * Reads the latest value from the persistent store.
     */
    OperationResult<T> read(ConsistencyContext context);

    /**
     * Normalized result wrapper so the protocol can distinguish failures from version rejection.
     */
    final class OperationResult<R> {
        private final OperationStatus status;
        private final R data;

        private OperationResult(OperationStatus status, R data) {
            this.status = status;
            this.data = data;
        }

        public static <R> OperationResult<R> success(R data) {
            return new OperationResult<R>(OperationStatus.SUCCESS, data);
        }

        public static <R> OperationResult<R> success() {
            return new OperationResult<R>(OperationStatus.SUCCESS, null);
        }

        public static <R> OperationResult<R> failure() {
            return new OperationResult<R>(OperationStatus.FAILURE, null);
        }

        public static <R> OperationResult<R> versionRejected() {
            return new OperationResult<R>(OperationStatus.VERSION_REJECTED, null);
        }

        public static <R> OperationResult<R> versionMissing() {
            return new OperationResult<R>(OperationStatus.VERSION_MISSING, null);
        }

        public OperationStatus getStatus() {
            return status;
        }

        public R getData() {
            return data;
        }
    }

    /**
     * Normalized operation outcomes understood by the consistency client.
     */
    enum OperationStatus {
        SUCCESS,
        FAILURE,
        VERSION_REJECTED,
        VERSION_MISSING
    }
}
