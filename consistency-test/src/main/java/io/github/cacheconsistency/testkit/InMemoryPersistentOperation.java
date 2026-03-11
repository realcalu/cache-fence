package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.BatchPersistentOperation;
import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.PersistentOperation;
import io.github.cacheconsistency.core.TransactionalPersistentOperation;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InMemoryPersistentOperation<T> implements TransactionalPersistentOperation<T>, BatchPersistentOperation<T> {
    private final Map<String, VersionedValue<T>> storage = new ConcurrentHashMap<String, VersionedValue<T>>();
    private final AtomicInteger readCount = new AtomicInteger();
    private final AtomicInteger updateCount = new AtomicInteger();
    private final AtomicInteger batchReadCount = new AtomicInteger();
    private final AtomicInteger batchUpdateCount = new AtomicInteger();
    private final AtomicInteger batchDeleteCount = new AtomicInteger();
    private final AtomicInteger batchQueryVersionCount = new AtomicInteger();
    private volatile long readDelayMillis;
    private volatile long updateDelayMillis;
    private volatile Runnable beforeUpdateHook;
    private volatile RuntimeException readFailure;
    private volatile RuntimeException queryVersionFailure;
    private volatile RuntimeException updateFailure;
    private volatile RuntimeException deleteFailure;

    @Override
    public OperationResult<Void> update(ConsistencyContext context) {
        updateCount.incrementAndGet();
        if (beforeUpdateHook != null) {
            beforeUpdateHook.run();
        }
        if (updateFailure != null) {
            throw updateFailure;
        }
        delayUpdateIfNecessary();
        String key = context.getKey();
        VersionedValue<T> current = storage.get(key);
        if (!versionMatches(context.getVersion(), current)) {
            return OperationResult.versionRejected();
        }

        long nextVersion = current == null ? 1L : current.version + 1;
        storage.put(key, new VersionedValue<T>(context.getValue(), nextVersion));
        return OperationResult.success();
    }

    @Override
    public OperationResult<Void> delete(ConsistencyContext context) {
        if (deleteFailure != null) {
            throw deleteFailure;
        }
        String key = context.getKey();
        VersionedValue<T> current = storage.get(key);
        if (!versionMatches(context.getVersion(), current)) {
            return OperationResult.versionRejected();
        }
        storage.remove(key);
        return OperationResult.success();
    }

    @Override
    public OperationResult<String> queryVersion(ConsistencyContext context) {
        if (queryVersionFailure != null) {
            throw queryVersionFailure;
        }
        VersionedValue<T> current = storage.get(context.getKey());
        if (current == null) {
            return OperationResult.versionMissing();
        }
        return OperationResult.success(String.valueOf(current.version));
    }

    @Override
    public OperationResult<T> read(ConsistencyContext context) {
        readCount.incrementAndGet();
        if (readFailure != null) {
            throw readFailure;
        }
        delayIfNecessary();
        VersionedValue<T> current = storage.get(context.getKey());
        return OperationResult.success(current == null ? null : current.value);
    }

    @Override
    public Map<String, OperationResult<T>> readAll(Collection<ConsistencyContext> contexts) {
        batchReadCount.incrementAndGet();
        delayIfNecessary();
        Map<String, OperationResult<T>> result = new LinkedHashMap<String, OperationResult<T>>();
        for (ConsistencyContext context : contexts) {
            VersionedValue<T> current = storage.get(context.getKey());
            result.put(context.getKey(), OperationResult.success(current == null ? null : current.value));
        }
        return result;
    }

    @Override
    public Map<String, OperationResult<String>> queryVersionAll(Collection<ConsistencyContext> contexts) {
        batchQueryVersionCount.incrementAndGet();
        Map<String, OperationResult<String>> result = new LinkedHashMap<String, OperationResult<String>>();
        for (ConsistencyContext context : contexts) {
            VersionedValue<T> current = storage.get(context.getKey());
            result.put(context.getKey(), current == null
                    ? OperationResult.<String>versionMissing()
                    : OperationResult.success(String.valueOf(current.version)));
        }
        return result;
    }

    @Override
    public Map<String, OperationResult<Void>> updateAll(Collection<ConsistencyContext> contexts) {
        batchUpdateCount.incrementAndGet();
        delayUpdateIfNecessary();
        Map<String, OperationResult<Void>> result = new LinkedHashMap<String, OperationResult<Void>>();
        for (ConsistencyContext context : contexts) {
            String key = context.getKey();
            VersionedValue<T> current = storage.get(key);
            if (!versionMatches(context.getVersion(), current)) {
                result.put(key, OperationResult.<Void>versionRejected());
                continue;
            }
            long nextVersion = current == null ? 1L : current.version + 1;
            storage.put(key, new VersionedValue<T>(context.getValue(), nextVersion));
            result.put(key, OperationResult.<Void>success());
        }
        return result;
    }

    @Override
    public Map<String, OperationResult<Void>> deleteAll(Collection<ConsistencyContext> contexts) {
        batchDeleteCount.incrementAndGet();
        Map<String, OperationResult<Void>> result = new LinkedHashMap<String, OperationResult<Void>>();
        for (ConsistencyContext context : contexts) {
            String key = context.getKey();
            VersionedValue<T> current = storage.get(key);
            if (!versionMatches(context.getVersion(), current)) {
                result.put(key, OperationResult.<Void>versionRejected());
                continue;
            }
            storage.remove(key);
            result.put(key, OperationResult.<Void>success());
        }
        return result;
    }

    public void seed(String key, T value, long version) {
        storage.put(key, new VersionedValue<T>(value, version));
    }

    public T getValue(String key) {
        VersionedValue<T> current = storage.get(key);
        return current == null ? null : current.value;
    }

    public String getVersion(String key) {
        VersionedValue<T> current = storage.get(key);
        return current == null ? null : String.valueOf(current.version);
    }

    public int getReadCount() {
        return readCount.get();
    }

    public int getBatchReadCount() {
        return batchReadCount.get();
    }

    public int getUpdateCount() {
        return updateCount.get();
    }

    public int getBatchUpdateCount() {
        return batchUpdateCount.get();
    }

    public int getBatchDeleteCount() {
        return batchDeleteCount.get();
    }

    public int getBatchQueryVersionCount() {
        return batchQueryVersionCount.get();
    }

    public void setReadDelayMillis(long readDelayMillis) {
        this.readDelayMillis = readDelayMillis;
    }

    public void setUpdateDelayMillis(long updateDelayMillis) {
        this.updateDelayMillis = updateDelayMillis;
    }

    public void setBeforeUpdateHook(Runnable beforeUpdateHook) {
        this.beforeUpdateHook = beforeUpdateHook;
    }

    public void setReadFailure(RuntimeException readFailure) {
        this.readFailure = readFailure;
    }

    public void setQueryVersionFailure(RuntimeException queryVersionFailure) {
        this.queryVersionFailure = queryVersionFailure;
    }

    public void setUpdateFailure(RuntimeException updateFailure) {
        this.updateFailure = updateFailure;
    }

    public void setDeleteFailure(RuntimeException deleteFailure) {
        this.deleteFailure = deleteFailure;
    }

    public void bumpVersion(String key) {
        VersionedValue<T> current = storage.get(key);
        if (current == null) {
            storage.put(key, new VersionedValue<T>(null, 1L));
            return;
        }
        storage.put(key, new VersionedValue<T>(current.value, current.version + 1));
    }

    @Override
    public <R> R executeInTransaction(ConsistencyContext context, TransactionCallback<R> callback) {
        return callback.doInTransaction();
    }

    private void delayIfNecessary() {
        if (readDelayMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(readDelayMillis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private void delayUpdateIfNecessary() {
        if (updateDelayMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(updateDelayMillis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean versionMatches(String expectedVersion, VersionedValue<T> current) {
        if (expectedVersion == null || expectedVersion.trim().isEmpty()) {
            return true;
        }
        if (current == null) {
            return false;
        }
        return expectedVersion.equals(String.valueOf(current.version));
    }

    private static final class VersionedValue<T> {
        private final T value;
        private final long version;

        private VersionedValue(T value, long version) {
            this.value = value;
            this.version = version;
        }
    }
}
