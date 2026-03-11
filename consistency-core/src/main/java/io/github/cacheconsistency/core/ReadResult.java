package io.github.cacheconsistency.core;

/**
 * Read outcome including the data source used for the result.
 */
public final class ReadResult<T> {
    /**
     * Distinguishes whether the value came from cache or the backing store.
     */
    public enum ReadSource {
        CACHE,
        STORE
    }

    private final T data;
    private final ReadSource source;

    private ReadResult(T data, ReadSource source) {
        this.data = data;
        this.source = source;
    }

    public static <T> ReadResult<T> fromCache(T data) {
        return new ReadResult<T>(data, ReadSource.CACHE);
    }

    public static <T> ReadResult<T> fromStore(T data) {
        return new ReadResult<T>(data, ReadSource.STORE);
    }

    public T getData() {
        return data;
    }

    public ReadSource getSource() {
        return source;
    }
}
