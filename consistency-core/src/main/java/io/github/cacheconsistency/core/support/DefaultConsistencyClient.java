package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.CommandType;
import io.github.cacheconsistency.core.BatchDeleteCommand;
import io.github.cacheconsistency.core.BatchPersistentOperation;
import io.github.cacheconsistency.core.BatchSetCommand;
import io.github.cacheconsistency.core.ConsistencyClient;
import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.ConsistencySettings;
import io.github.cacheconsistency.core.PersistentOperation;
import io.github.cacheconsistency.core.ReadResult;
import io.github.cacheconsistency.core.Serializer;
import io.github.cacheconsistency.core.TransactionalPersistentOperation;
import io.github.cacheconsistency.core.WriteResult;
import io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import io.github.cacheconsistency.core.failover.ConservativeFailoverStrategy;
import io.github.cacheconsistency.core.failover.FailoverStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of the protocol client.
 *
 * <p>The client follows the protocol described by the project:
 * cache reads observe {@code status}/{@code validity}, writes perform a Lua prewrite,
 * execute the store mutation with version checks, then finalize the Redis state.</p>
 */
public class DefaultConsistencyClient<T> implements ConsistencyClient<T> {
    private static final Logger log = LoggerFactory.getLogger(DefaultConsistencyClient.class);

    private final RedisAccessor redisAccessor;
    private final PersistentOperation<T> persistentOperation;
    private final Serializer<T> serializer;
    private final ConsistencySettings settings;
    private final ProtocolKeys protocolKeys;
    private final ConsistencyObserver observer;
    private final GhostWriteHealer ghostWriteHealer;
    private final FinalizeFailureHandler finalizeFailureHandler;
    private final ExecutorService batchExecutorService;

    public DefaultConsistencyClient(RedisAccessor redisAccessor,
                                    PersistentOperation<T> persistentOperation,
                                    Serializer<T> serializer,
                                    ConsistencySettings settings) {
        this(redisAccessor, persistentOperation, serializer, settings,
                ConsistencyObserver.NoOpConsistencyObserver.INSTANCE,
                GhostWriteHealer.NoOpGhostWriteHealer.INSTANCE,
                FinalizeFailureHandler.NoOpFinalizeFailureHandler.INSTANCE);
    }

    public DefaultConsistencyClient(RedisAccessor redisAccessor,
                                    PersistentOperation<T> persistentOperation,
                                    Serializer<T> serializer,
                                    ConsistencySettings settings,
                                    ConsistencyObserver observer) {
        this(redisAccessor, persistentOperation, serializer, settings,
                observer,
                GhostWriteHealer.NoOpGhostWriteHealer.INSTANCE,
                FinalizeFailureHandler.NoOpFinalizeFailureHandler.INSTANCE,
                false);
    }

    public DefaultConsistencyClient(RedisAccessor redisAccessor,
                                    PersistentOperation<T> persistentOperation,
                                    Serializer<T> serializer,
                                    ConsistencySettings settings,
                                    ConsistencyObserver observer,
                                    GhostWriteHealer ghostWriteHealer) {
        this(redisAccessor, persistentOperation, serializer, settings,
                observer,
                ghostWriteHealer,
                FinalizeFailureHandler.NoOpFinalizeFailureHandler.INSTANCE,
                false);
    }

    public DefaultConsistencyClient(RedisAccessor redisAccessor,
                                    PersistentOperation<T> persistentOperation,
                                    Serializer<T> serializer,
                                    ConsistencySettings settings,
                                    ConsistencyObserver observer,
                                    GhostWriteHealer ghostWriteHealer,
                                    FinalizeFailureHandler finalizeFailureHandler) {
        this(redisAccessor, persistentOperation, serializer, settings,
                observer, ghostWriteHealer, finalizeFailureHandler, false);
    }

    private DefaultConsistencyClient(RedisAccessor redisAccessor,
                                     PersistentOperation<T> persistentOperation,
                                     Serializer<T> serializer,
                                     ConsistencySettings settings,
                                     ConsistencyObserver observer,
                                     GhostWriteHealer ghostWriteHealer,
                                     FinalizeFailureHandler finalizeFailureHandler,
                                     boolean ignored) {
        this.redisAccessor = redisAccessor;
        this.persistentOperation = persistentOperation;
        this.serializer = serializer;
        this.settings = settings;
        this.protocolKeys = new ProtocolKeys(settings.getKeyPrefix());
        this.observer = observer;
        this.ghostWriteHealer = ghostWriteHealer;
        this.finalizeFailureHandler = finalizeFailureHandler;
        this.batchExecutorService = settings.getBatchParallelism() <= 1
                ? null
                : new ThreadPoolExecutor(
                        settings.getBatchParallelism(),
                        settings.getBatchParallelism(),
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new NamedThreadFactory("cck-batch"));
    }

    @Override
    public ReadResult<T> get(String key, Duration ttl, ConsistencyContext context) {
        requireKey(key);
        requireTtl(ttl);
        ConsistencyContext scoped = scope(context, key, ttl, CommandType.GET, null);
        try {
            byte[] cached = redisAccessor.get(cacheKey(key));
            if (cached != null) {
                // Cached data can exist while the protocol explicitly marks it invalid.
                if (isInvalidCache(key)) {
                    observer.onCacheMiss(key);
                    return readFromStoreAndOptionallyPopulate(key, ttl, scoped, false);
                }
                observer.onCacheHit(key);
                return ReadResult.fromCache(serializer.deserialize(cached));
            }
            observer.onCacheMiss(key);
        } catch (RuntimeException error) {
            if (!failover(scoped).allowStoreReadOnCacheFailure(scoped, error)) {
                throw error;
            }
            log.warn("Cache read failed for key {}", key, error);
            return readFromStoreAndOptionallyPopulate(key, ttl, scoped, false);
        }

        String status = readStatus(key);
        if (ProtocolState.IN_WRITING.value().equals(status)) {
            // A writer is still inside the protocol window, so the read must bypass cache.
            return readFromStoreAndOptionallyPopulate(key, ttl, scoped, false);
        }
        if (status == null) {
            // Missing protocol metadata is treated as a possible ghost-write window.
            triggerGhostWriteHealIfEnabled(key, scoped);
            return readFromStoreAndOptionallyPopulate(key, ttl, scoped, false);
        }

        String token = UUID.randomUUID().toString();
        boolean leaseAcquired = redisAccessor.setIfAbsent(leaseKey(key), token.getBytes(StandardCharsets.UTF_8),
                Duration.ofSeconds(settings.getLeaseTtlSeconds()));
        if (leaseAcquired) {
            observer.onLeaseAcquired(key);
            try {
                return readFromStoreAndOptionallyPopulate(key, ttl, scoped, true);
            } finally {
                releaseLock(leaseKey(key), token);
            }
        }

        if (settings.getRetryBackoffMillis() > 0) {
            try {
                Thread.sleep(settings.getRetryBackoffMillis());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
        }

        try {
            byte[] cached = redisAccessor.get(cacheKey(key));
            if (cached != null) {
                if (isInvalidCache(key)) {
                    observer.onCacheMiss(key);
                    return readFromStoreAndOptionallyPopulate(key, ttl, scoped, false);
                }
                observer.onCacheHit(key);
                return ReadResult.fromCache(serializer.deserialize(cached));
            }
            observer.onCacheMiss(key);
        } catch (RuntimeException error) {
            if (!failover(scoped).allowStoreReadOnCacheFailure(scoped, error)) {
                throw error;
            }
            log.warn("Cache retry read failed for key {}", key, error);
        }
        return readFromStoreAndOptionallyPopulate(key, ttl, scoped, false);
    }

    @Override
    public WriteResult set(String key, T value, Duration ttl, ConsistencyContext context) {
        requireKey(key);
        requireTtl(ttl);
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }

        ConsistencyContext scoped = scope(context, key, ttl, CommandType.SET, value);
        String token = UUID.randomUUID().toString();
        boolean locked = redisAccessor.beginWrite(statusKey(key), protocolLeaseKey(key), cacheKey(key), validityKey(key),
                token.getBytes(StandardCharsets.UTF_8),
                Duration.ofSeconds(settings.getWriteLockTtlSeconds()));
        if (!locked) {
            return WriteResult.of(WriteResult.WriteStatus.WRITE_LOCK_BUSY);
        }

        try {
            return executeWithinTransaction(scoped, new TransactionAction<WriteResult>() {
                @Override
                public WriteResult execute() {
                    WriteResult versionCheckFailure = verifyExpectedVersion(scoped);
                    if (versionCheckFailure != null) {
                        observer.onVersionRejected(key);
                        rollbackProtocolState(key, token);
                        return versionCheckFailure;
                    }
                    PersistentOperation.OperationResult<Void> result = persistentOperation.update(scoped);
                    if (result.getStatus() == PersistentOperation.OperationStatus.VERSION_REJECTED) {
                        observer.onVersionRejected(key);
                        return WriteResult.of(WriteResult.WriteStatus.VERSION_REJECTED);
                    }
                    if (result.getStatus() != PersistentOperation.OperationStatus.SUCCESS) {
                        rollbackProtocolState(key, token);
                        throw new IllegalStateException("Persistent store update failed");
                    }
                    observer.onStoreWrite(key);
                    try {
                        byte[] serializedValue = serializer.serialize(value);
                        // Cache the new value only while holding the protocol lease that started the write.
                        boolean staged = redisAccessor.stageCacheValue(protocolLeaseKey(key),
                                token.getBytes(StandardCharsets.UTF_8), cacheKey(key), serializedValue, ttl);
                        if (!staged) {
                            return WriteResult.of(WriteResult.WriteStatus.STORE_UPDATED);
                        }
                        // Finalize the protocol state only after the new cache value has been staged.
                        try {
                            redisAccessor.finalizeWrite(statusKey(key), protocolLeaseKey(key), validityKey(key), cacheKey(key),
                                    token.getBytes(StandardCharsets.UTF_8),
                                    Duration.ofSeconds(settings.getWriteLockTtlSeconds()), ttl);
                        } catch (RuntimeException error) {
                            return handleSetFinalizeFailure(key, cacheKey(key), serializedValue, ttl, scoped, error);
                        }
                        return WriteResult.of(WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED);
                    } catch (RuntimeException error) {
                        throw new IllegalStateException("Protocol write stage failed after store update", error);
                    }
                }
            });
        } finally {
            releaseLock(protocolLeaseKey(key), token);
        }
    }

    @Override
    public WriteResult delete(String key, ConsistencyContext context) {
        requireKey(key);
        ConsistencyContext scoped = scope(context, key, null, CommandType.DELETE, null);
        String token = UUID.randomUUID().toString();
        boolean locked = redisAccessor.beginWrite(statusKey(key), protocolLeaseKey(key), cacheKey(key), validityKey(key),
                token.getBytes(StandardCharsets.UTF_8),
                Duration.ofSeconds(settings.getWriteLockTtlSeconds()));
        if (!locked) {
            return WriteResult.of(WriteResult.WriteStatus.WRITE_LOCK_BUSY);
        }

        try {
            return executeWithinTransaction(scoped, new TransactionAction<WriteResult>() {
                @Override
                public WriteResult execute() {
                    WriteResult versionCheckFailure = verifyExpectedVersion(scoped);
                    if (versionCheckFailure != null) {
                        observer.onVersionRejected(key);
                        rollbackProtocolState(key, token);
                        return versionCheckFailure;
                    }
                    PersistentOperation.OperationResult<Void> result = persistentOperation.delete(scoped);
                    if (result.getStatus() == PersistentOperation.OperationStatus.VERSION_REJECTED) {
                        observer.onVersionRejected(key);
                        return WriteResult.of(WriteResult.WriteStatus.VERSION_REJECTED);
                    }
                    if (result.getStatus() != PersistentOperation.OperationStatus.SUCCESS) {
                        rollbackProtocolState(key, token);
                        throw new IllegalStateException("Persistent store delete failed");
                    }
                    observer.onStoreDelete(key);
                    try {
                        try {
                            redisAccessor.finalizeWrite(statusKey(key), protocolLeaseKey(key), validityKey(key), cacheKey(key),
                                    token.getBytes(StandardCharsets.UTF_8),
                                    Duration.ofSeconds(settings.getWriteLockTtlSeconds()), Duration.ofSeconds(1));
                        } catch (RuntimeException error) {
                            return handleDeleteFinalizeFailure(key, cacheKey(key), scoped, error);
                        }
                        redisAccessor.delete(validityKey(key));
                        return WriteResult.of(WriteResult.WriteStatus.DELETED);
                    } catch (RuntimeException error) {
                        throw new IllegalStateException("Protocol delete stage failed after store delete", error);
                    }
                }
            });
        } finally {
            releaseLock(protocolLeaseKey(key), token);
        }
    }

    private ReadResult<T> readFromStoreAndOptionallyPopulate(String key,
                                                             Duration ttl,
                                                             ConsistencyContext context,
                                                             boolean populateCache) {
        PersistentOperation.OperationResult<T> result = persistentOperation.read(context);
        if (result.getStatus() != PersistentOperation.OperationStatus.SUCCESS) {
            throw new IllegalStateException("Persistent store read failed");
        }
        observer.onStoreRead(key);
        T value = result.getData();
        if (value != null && populateCache) {
            try {
                redisAccessor.set(cacheKey(key), serializer.serialize(value), ttl);
            } catch (RuntimeException error) {
                if (!failover(context).allowCacheWriteSkipOnFailure(context, error)) {
                    throw error;
                }
                log.warn("Cache populate skipped for key {}", key, error);
            }
        }
        return ReadResult.fromStore(value);
    }

    @Override
    public Map<String, ReadResult<T>> getAll(Collection<String> keys, Duration ttl, ConsistencyContext context) {
        boolean optimized = redisAccessor instanceof BatchRedisAccessor && persistentOperation instanceof BatchPersistentOperation;
        observer.onBatchOperation("get", keys.size(), optimized);
        if (optimized) {
            return getAllOptimized(new ArrayList<String>(keys), ttl, context,
                    (BatchRedisAccessor) redisAccessor,
                    (BatchPersistentOperation<T>) persistentOperation);
        }
        return executeBatch(new ArrayList<String>(keys), new BatchAction<String, ReadResult<T>>() {
            @Override
            public ReadResult<T> execute(String key, ConsistencyContext scopedContext) {
                return get(key, ttl, scopedContext);
            }
        }, context, new BatchContextResolver<String>() {
            @Override
            public ConsistencyContext resolve(String item, ConsistencyContext batchContext) {
                return batchContext == null ? null : batchContext.copy();
            }
        });
    }

    @Override
    public Map<String, WriteResult> setAll(Map<String, T> values, Duration ttl, ConsistencyContext context) {
        List<BatchSetCommand<T>> commands = new ArrayList<BatchSetCommand<T>>(values.size());
        for (Map.Entry<String, T> entry : values.entrySet()) {
            commands.add(BatchSetCommand.of(entry.getKey(), entry.getValue(), null));
        }
        return setAllWithContexts(commands, ttl, context);
    }

    @Override
    public Map<String, WriteResult> setAllWithContexts(Collection<BatchSetCommand<T>> commands,
                                                       Duration ttl,
                                                       ConsistencyContext defaultContext) {
        return toWriteResults(setAllWithContextsDetailed(commands, ttl, defaultContext));
    }

    @Override
    public Map<String, BatchWriteDebugSnapshot> setAllWithContextsDetailed(Collection<BatchSetCommand<T>> commands,
                                                                           Duration ttl,
                                                                           ConsistencyContext defaultContext) {
        boolean optimized = persistentOperation instanceof BatchPersistentOperation;
        observer.onBatchOperation("set", commands.size(), optimized);
        List<BatchSetCommand<T>> items = new ArrayList<BatchSetCommand<T>>(commands);
        if (optimized) {
            return setAllOptimized(items, ttl, defaultContext,
                    (BatchPersistentOperation<T>) persistentOperation);
        }
        return executeBatch(items, new BatchAction<BatchSetCommand<T>, BatchWriteDebugSnapshot>() {
            @Override
            public BatchWriteDebugSnapshot execute(BatchSetCommand<T> item, ConsistencyContext scopedContext) {
                WriteResult result = set(item.getKey(), item.getValue(), ttl, scopedContext);
                return debugSnapshotForSetResult(item.getKey(), result.getStatus(), false);
            }

            @Override
            public String keyOf(BatchSetCommand<T> item) {
                return item.getKey();
            }
        }, defaultContext, new BatchContextResolver<BatchSetCommand<T>>() {
            @Override
            public ConsistencyContext resolve(BatchSetCommand<T> item, ConsistencyContext batchContext) {
                return ConsistencyContext.merge(batchContext, item.getContext());
            }
        });
    }

    @Override
    public Map<String, WriteResult> deleteAll(Collection<String> keys, ConsistencyContext context) {
        List<BatchDeleteCommand> commands = new ArrayList<BatchDeleteCommand>(keys.size());
        for (String key : keys) {
            commands.add(BatchDeleteCommand.of(key, null));
        }
        return deleteAllWithContexts(commands, context);
    }

    @Override
    public Map<String, WriteResult> deleteAllWithContexts(Collection<BatchDeleteCommand> commands,
                                                          ConsistencyContext defaultContext) {
        return toWriteResults(deleteAllWithContextsDetailed(commands, defaultContext));
    }

    @Override
    public Map<String, BatchWriteDebugSnapshot> deleteAllWithContextsDetailed(Collection<BatchDeleteCommand> commands,
                                                                              ConsistencyContext defaultContext) {
        boolean optimized = persistentOperation instanceof BatchPersistentOperation;
        observer.onBatchOperation("delete", commands.size(), optimized);
        List<BatchDeleteCommand> items = new ArrayList<BatchDeleteCommand>(commands);
        if (optimized) {
            return deleteAllOptimized(items, defaultContext,
                    (BatchPersistentOperation<T>) persistentOperation);
        }
        return executeBatch(items, new BatchAction<BatchDeleteCommand, BatchWriteDebugSnapshot>() {
            @Override
            public BatchWriteDebugSnapshot execute(BatchDeleteCommand item, ConsistencyContext scopedContext) {
                WriteResult result = delete(item.getKey(), scopedContext);
                return debugSnapshotForDeleteResult(item.getKey(), result.getStatus(), false);
            }
            
            @Override
            public String keyOf(BatchDeleteCommand item) {
                return item.getKey();
            }
        }, defaultContext, new BatchContextResolver<BatchDeleteCommand>() {
            @Override
            public ConsistencyContext resolve(BatchDeleteCommand item, ConsistencyContext batchContext) {
                return ConsistencyContext.merge(batchContext, item.getContext());
            }
        });
    }

    private ConsistencyContext scope(ConsistencyContext context,
                                     String key,
                                     Duration ttl,
                                     CommandType commandType,
                                     Object value) {
        ConsistencyContext target = context == null ? ConsistencyContext.create() : context.copy();
        target.withKey(key).withCommandType(commandType).withValue(value);
        if (ttl != null) {
            target.withTtlSeconds(ttl.getSeconds());
        }
        return target;
    }

    private FailoverStrategy failover(ConsistencyContext context) {
        return context.getFailoverStrategy() == null ? ConservativeFailoverStrategy.INSTANCE : context.getFailoverStrategy();
    }

    private WriteResult verifyExpectedVersion(ConsistencyContext context) {
        if (context.getVersion() == null || context.getVersion().trim().isEmpty()) {
            return null;
        }
        PersistentOperation.OperationResult<String> versionResult = persistentOperation.queryVersion(context);
        if (versionResult.getStatus() == PersistentOperation.OperationStatus.VERSION_MISSING) {
            return WriteResult.of(WriteResult.WriteStatus.VERSION_REJECTED);
        }
        if (versionResult.getStatus() != PersistentOperation.OperationStatus.SUCCESS) {
            throw new IllegalStateException("Persistent store version query failed");
        }
        if (!context.getVersion().equals(versionResult.getData())) {
            return WriteResult.of(WriteResult.WriteStatus.VERSION_REJECTED);
        }
        return null;
    }

    private void releaseLock(String lockKey, String token) {
        try {
            redisAccessor.compareAndDelete(lockKey, token.getBytes(StandardCharsets.UTF_8));
        } catch (RuntimeException error) {
            log.warn("Safe lock release failed for key {}", lockKey, error);
        }
    }

    private String cacheKey(String key) {
        return protocolKeys.data(key);
    }

    private String leaseKey(String key) {
        return protocolKeys.lease(key);
    }

    private String protocolLeaseKey(String key) {
        return protocolKeys.protocolLease(key);
    }

    private String statusKey(String key) {
        return protocolKeys.status(key);
    }

    private String validityKey(String key) {
        return protocolKeys.validity(key);
    }

    private boolean isInvalidCache(String key) {
        byte[] validityBytes = redisAccessor.get(validityKey(key));
        if (validityBytes == null) {
            return false;
        }
        ProtocolValidity validity = ProtocolValidity.fromValue(new String(validityBytes, StandardCharsets.UTF_8));
        return validity == ProtocolValidity.INVALID;
    }

    private String readStatus(String key) {
        byte[] statusBytes = redisAccessor.get(statusKey(key));
        if (statusBytes == null) {
            return null;
        }
        ProtocolState state = ProtocolState.fromValue(new String(statusBytes, StandardCharsets.UTF_8));
        return state == null ? null : state.value();
    }

    private void rollbackProtocolState(String key, String token) {
        redisAccessor.rollbackWrite(statusKey(key), protocolLeaseKey(key), validityKey(key),
                token.getBytes(StandardCharsets.UTF_8));
    }

    private void triggerGhostWriteHealIfEnabled(String key, ConsistencyContext context) {
        if (!settings.isGhostWriteHealingEnabled()) {
            return;
        }
        ghostWriteHealer.scheduleHeal(key, context.copy());
    }

    private WriteResult handleSetFinalizeFailure(String key,
                                                 String cacheKey,
                                                 byte[] cacheValue,
                                                 Duration ttl,
                                                 ConsistencyContext context,
                                                 RuntimeException error) {
        if (finalizeFailureHandler == FinalizeFailureHandler.NoOpFinalizeFailureHandler.INSTANCE) {
            throw error;
        }
        observer.onFinalizeFailure(key, "set", error);
        log.warn("Finalize failed for set key {}, delegating to optional extension", key, error);
        finalizeFailureHandler.onSetFinalizeFailure(key, cacheKey, cacheValue, ttl, context.copy(), error);
        return WriteResult.of(WriteResult.WriteStatus.STORE_UPDATED_CACHE_REPAIR_SCHEDULED);
    }

    private WriteResult handleDeleteFinalizeFailure(String key,
                                                    String cacheKey,
                                                    ConsistencyContext context,
                                                    RuntimeException error) {
        if (finalizeFailureHandler == FinalizeFailureHandler.NoOpFinalizeFailureHandler.INSTANCE) {
            throw error;
        }
        observer.onFinalizeFailure(key, "delete", error);
        log.warn("Finalize failed for delete key {}, delegating to optional extension", key, error);
        finalizeFailureHandler.onDeleteFinalizeFailure(key, cacheKey, context.copy(), error);
        return WriteResult.of(WriteResult.WriteStatus.DELETED_CACHE_REPAIR_SCHEDULED);
    }

    private <R> R executeWithinTransaction(ConsistencyContext context, TransactionAction<R> action) {
        if (persistentOperation instanceof TransactionalPersistentOperation) {
            // Let the store own the actual transaction implementation while the protocol owns the call sequence.
            TransactionalPersistentOperation<T> transactional = (TransactionalPersistentOperation<T>) persistentOperation;
            return transactional.executeInTransaction(context, new TransactionalPersistentOperation.TransactionCallback<R>() {
                @Override
                public R doInTransaction() {
                    return action.execute();
                }
            });
        }
        return action.execute();
    }

    private <I, R> Map<String, R> executeBatch(List<I> items,
                                               BatchAction<I, R> action,
                                               ConsistencyContext context,
                                               BatchContextResolver<I> contextResolver) {
        if (settings.getBatchParallelism() <= 1 || items.size() <= 1) {
            Map<String, R> result = new LinkedHashMap<String, R>();
            for (I item : items) {
                result.put(action.keyOf(item), action.execute(item, contextResolver.resolve(item, context)));
            }
            return result;
        }

        List<Future<R>> futures = new ArrayList<Future<R>>(items.size());
        for (I item : items) {
            final I batchItem = item;
            final ConsistencyContext scopedContext = contextResolver.resolve(batchItem, context);
            futures.add(batchExecutorService.submit(new Callable<R>() {
                @Override
                public R call() {
                    return action.execute(batchItem, scopedContext);
                }
            }));
        }
        Map<String, R> result = new LinkedHashMap<String, R>();
        for (int i = 0; i < items.size(); i++) {
            try {
                result.put(action.keyOf(items.get(i)), futures.get(i).get());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Batch execution interrupted", interruptedException);
            } catch (ExecutionException executionException) {
                Throwable cause = executionException.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new IllegalStateException("Batch execution failed", cause);
            }
        }
        return result;
    }

    private Map<String, ReadResult<T>> getAllOptimized(List<String> keys,
                                                       Duration ttl,
                                                       ConsistencyContext context,
                                                       BatchRedisAccessor batchRedisAccessor,
                                                       BatchPersistentOperation<T> batchPersistentOperation) {
        requireTtl(ttl);
        for (String key : keys) {
            requireKey(key);
        }
        Map<String, ReadResult<T>> result = new LinkedHashMap<String, ReadResult<T>>();
        Map<String, ConsistencyContext> contexts = new LinkedHashMap<String, ConsistencyContext>();
        for (String key : keys) {
            contexts.put(key, scope(context, key, ttl, CommandType.GET, null));
        }
        Map<String, BatchRedisAccessor.ReadProtocolResult> protocolResults =
                batchRedisAccessor.readProtocolAll(readProtocolRequests(keys));
        List<ConsistencyContext> storeReads = new ArrayList<ConsistencyContext>();
        Map<String, Boolean> populateCache = new HashMap<String, Boolean>();
        Map<String, String> leaseTokens = new HashMap<String, String>();
        List<String> leaseRetryKeys = new ArrayList<String>();
        for (String key : keys) {
            BatchRedisAccessor.ReadProtocolResult protocolResult = protocolResults.get(key);
            if (protocolResult == null) {
                throw new IllegalStateException("Batch Redis read protocol result missing");
            }
            if (protocolResult.getDecision() == io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot.Decision.CACHE_HIT) {
                observer.onCacheHit(key);
                result.put(key, ReadResult.fromCache(serializer.deserialize(protocolResult.getCachedValue())));
                continue;
            }
            observer.onCacheMiss(key);
            if (protocolResult.getDecision() == io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot.Decision.INVALID_CACHE) {
                storeReads.add(contexts.get(key));
                populateCache.put(key, Boolean.FALSE);
                continue;
            }
            if (protocolResult.getDecision() == io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot.Decision.IN_WRITING) {
                storeReads.add(contexts.get(key));
                populateCache.put(key, Boolean.FALSE);
                continue;
            }
            if (protocolResult.getDecision() == io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot.Decision.STATUS_MISSING) {
                triggerGhostWriteHealIfEnabled(key, contexts.get(key));
                storeReads.add(contexts.get(key));
                populateCache.put(key, Boolean.FALSE);
                continue;
            }
            String token = UUID.randomUUID().toString();
            boolean leaseAcquired = redisAccessor.setIfAbsent(leaseKey(key), token.getBytes(StandardCharsets.UTF_8),
                    Duration.ofSeconds(settings.getLeaseTtlSeconds()));
            if (leaseAcquired) {
                observer.onLeaseAcquired(key);
                leaseTokens.put(key, token);
                storeReads.add(contexts.get(key));
                populateCache.put(key, Boolean.TRUE);
            } else {
                leaseRetryKeys.add(key);
            }
        }

        if (!leaseRetryKeys.isEmpty() && settings.getRetryBackoffMillis() > 0) {
            try {
                Thread.sleep(settings.getRetryBackoffMillis());
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
            }
            Map<String, byte[]> retryCachedValues = batchRedisAccessor.getAll(cacheKeys(leaseRetryKeys));
            Map<String, byte[]> retryValidityValues = batchRedisAccessor.getAll(validityKeys(leaseRetryKeys));
            for (String key : leaseRetryKeys) {
                byte[] cached = retryCachedValues.get(cacheKey(key));
                byte[] validityBytes = retryValidityValues.get(validityKey(key));
                ProtocolValidity validity = validityBytes == null
                        ? null
                        : ProtocolValidity.fromValue(new String(validityBytes, StandardCharsets.UTF_8));
                if (cached != null && validity != ProtocolValidity.INVALID) {
                    observer.onCacheHit(key);
                    result.put(key, ReadResult.fromCache(serializer.deserialize(cached)));
                } else {
                    storeReads.add(contexts.get(key));
                    populateCache.put(key, Boolean.FALSE);
                }
            }
        } else {
            for (String key : leaseRetryKeys) {
                storeReads.add(contexts.get(key));
                populateCache.put(key, Boolean.FALSE);
            }
        }

        if (!storeReads.isEmpty()) {
            Map<String, PersistentOperation.OperationResult<T>> storeResults = batchPersistentOperation.readAll(storeReads);
            Map<String, byte[]> backfill = new LinkedHashMap<String, byte[]>();
            for (ConsistencyContext scopedContext : storeReads) {
                PersistentOperation.OperationResult<T> storeResult = storeResults.get(scopedContext.getKey());
                if (storeResult == null || storeResult.getStatus() != PersistentOperation.OperationStatus.SUCCESS) {
                    throw new IllegalStateException("Persistent store batch read failed");
                }
                observer.onStoreRead(scopedContext.getKey());
                T value = storeResult.getData();
                result.put(scopedContext.getKey(), ReadResult.fromStore(value));
                if (value != null && Boolean.TRUE.equals(populateCache.get(scopedContext.getKey()))) {
                    backfill.put(cacheKey(scopedContext.getKey()), serializer.serialize(value));
                }
            }
            if (!backfill.isEmpty()) {
                try {
                    batchRedisAccessor.setAll(backfill, ttl);
                } catch (RuntimeException error) {
                    for (ConsistencyContext scopedContext : storeReads) {
                        if (!Boolean.TRUE.equals(populateCache.get(scopedContext.getKey()))) {
                            continue;
                        }
                        if (!failover(scopedContext).allowCacheWriteSkipOnFailure(scopedContext, error)) {
                            throw error;
                        }
                    }
                    log.warn("Batch cache populate skipped", error);
                }
            }
        }

        for (Map.Entry<String, String> leaseEntry : leaseTokens.entrySet()) {
            releaseLock(leaseKey(leaseEntry.getKey()), leaseEntry.getValue());
        }
        return result;
    }

    private Map<String, BatchWriteDebugSnapshot> setAllOptimized(List<BatchSetCommand<T>> entries,
                                                                 Duration ttl,
                                                                 ConsistencyContext defaultContext,
                                                                 BatchPersistentOperation<T> batchPersistentOperation) {
        requireTtl(ttl);
        List<ConsistencyContext> pending = new ArrayList<ConsistencyContext>();
        Map<String, String> tokens = new HashMap<String, String>();
        Map<String, T> valuesByKey = new HashMap<String, T>();
        Map<String, BatchWriteDebugState> debugStates = new LinkedHashMap<String, BatchWriteDebugState>();
        BatchRedisAccessor batchRedisAccessor = redisAccessor instanceof BatchRedisAccessor
                ? (BatchRedisAccessor) redisAccessor
                : null;
        List<BatchRedisAccessor.BeginWriteRequest> beginRequests = new ArrayList<BatchRedisAccessor.BeginWriteRequest>();
        for (BatchSetCommand<T> entry : entries) {
            requireKey(entry.getKey());
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("value must not be null");
            }
            ConsistencyContext merged = ConsistencyContext.merge(defaultContext, entry.getContext());
            ConsistencyContext scoped = scope(merged, entry.getKey(), ttl, CommandType.SET, entry.getValue());
            String token = UUID.randomUUID().toString();
            debugStates.put(entry.getKey(), new BatchWriteDebugState(entry.getKey(), CommandType.SET, true));
            pending.add(scoped);
            tokens.put(entry.getKey(), token);
            valuesByKey.put(entry.getKey(), entry.getValue());
            if (batchRedisAccessor != null) {
                beginRequests.add(new BatchRedisAccessor.BeginWriteRequest(
                        entry.getKey(),
                        statusKey(entry.getKey()),
                        protocolLeaseKey(entry.getKey()),
                        cacheKey(entry.getKey()),
                        validityKey(entry.getKey()),
                        token.getBytes(StandardCharsets.UTF_8),
                        Duration.ofSeconds(settings.getWriteLockTtlSeconds())
                ));
            } else {
                boolean locked = redisAccessor.beginWrite(statusKey(entry.getKey()), protocolLeaseKey(entry.getKey()),
                        cacheKey(entry.getKey()), validityKey(entry.getKey()),
                        token.getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(settings.getWriteLockTtlSeconds()));
                if (!locked) {
                    markFinal(debugStates.get(entry.getKey()),
                            BatchWriteDebugSnapshot.StepStatus.FAILED,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            false,
                            WriteResult.WriteStatus.WRITE_LOCK_BUSY);
                } else {
                    debugStates.get(entry.getKey()).prepareStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                }
            }
        }
        if (batchRedisAccessor != null) {
            Map<String, Boolean> lockedResults = batchRedisAccessor.beginWriteAll(beginRequests);
            List<ConsistencyContext> lockedPending = new ArrayList<ConsistencyContext>();
            for (ConsistencyContext scoped : pending) {
                if (Boolean.TRUE.equals(lockedResults.get(scoped.getKey()))) {
                    debugStates.get(scoped.getKey()).prepareStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                    lockedPending.add(scoped);
                } else {
                    markFinal(debugStates.get(scoped.getKey()),
                            BatchWriteDebugSnapshot.StepStatus.FAILED,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            false,
                            WriteResult.WriteStatus.WRITE_LOCK_BUSY);
                }
            }
            pending = lockedPending;
        }
        if (pending.isEmpty()) {
            return toSnapshots(debugStates);
        }
        try {
            executeBatchWriteTransaction(pending, batchPersistentOperation, true, debugStates, tokens, defaultContext);
            if (batchRedisAccessor != null) {
                List<BatchRedisAccessor.StageCacheValueRequest> stageRequests = new ArrayList<BatchRedisAccessor.StageCacheValueRequest>();
                Map<String, byte[]> serializedValues = new HashMap<String, byte[]>();
                for (ConsistencyContext scoped : pending) {
                    if (debugStates.get(scoped.getKey()).finalStatus != null) {
                        continue;
                    }
                    observer.onStoreWrite(scoped.getKey());
                    byte[] serializedValue = serializer.serialize(valuesByKey.get(scoped.getKey()));
                    serializedValues.put(scoped.getKey(), serializedValue);
                    stageRequests.add(new BatchRedisAccessor.StageCacheValueRequest(
                            scoped.getKey(),
                            protocolLeaseKey(scoped.getKey()),
                            tokens.get(scoped.getKey()).getBytes(StandardCharsets.UTF_8),
                            cacheKey(scoped.getKey()),
                            serializedValue,
                            ttl
                    ));
                }
                Map<String, Boolean> stagedResults = batchRedisAccessor.stageCacheValueAll(stageRequests);
                List<BatchRedisAccessor.FinalizeWriteRequest> finalizeRequests = new ArrayList<BatchRedisAccessor.FinalizeWriteRequest>();
                for (ConsistencyContext scoped : pending) {
                    if (debugStates.get(scoped.getKey()).finalStatus != null) {
                        continue;
                    }
                    if (!Boolean.TRUE.equals(stagedResults.get(scoped.getKey()))) {
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.FAILED,
                                BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                                false,
                                WriteResult.WriteStatus.STORE_UPDATED);
                        continue;
                    }
                    debugStates.get(scoped.getKey()).stageStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                    finalizeRequests.add(new BatchRedisAccessor.FinalizeWriteRequest(
                            scoped.getKey(),
                            statusKey(scoped.getKey()),
                            protocolLeaseKey(scoped.getKey()),
                            validityKey(scoped.getKey()),
                            cacheKey(scoped.getKey()),
                            tokens.get(scoped.getKey()).getBytes(StandardCharsets.UTF_8),
                            Duration.ofSeconds(settings.getWriteLockTtlSeconds()),
                            ttl
                    ));
                }
                try {
                    Map<String, Boolean> finalizeResults = batchRedisAccessor.finalizeWriteAll(finalizeRequests);
                    for (ConsistencyContext scoped : pending) {
                        if (debugStates.get(scoped.getKey()).finalStatus != null) {
                            continue;
                        }
                        if (Boolean.TRUE.equals(finalizeResults.get(scoped.getKey()))) {
                            markFinal(debugStates.get(scoped.getKey()),
                                    debugStates.get(scoped.getKey()).prepareStatus,
                                    debugStates.get(scoped.getKey()).versionStatus,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    false,
                                    WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED);
                        } else {
                            markFinal(debugStates.get(scoped.getKey()),
                                    debugStates.get(scoped.getKey()).prepareStatus,
                                    debugStates.get(scoped.getKey()).versionStatus,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    BatchWriteDebugSnapshot.StepStatus.FAILED,
                                    false,
                                    WriteResult.WriteStatus.STORE_UPDATED);
                        }
                    }
                } catch (RuntimeException error) {
                    for (ConsistencyContext scoped : pending) {
                        if (debugStates.get(scoped.getKey()).finalStatus != null) {
                            continue;
                        }
                        WriteResult finalizeResult = handleSetFinalizeFailure(
                                scoped.getKey(), cacheKey(scoped.getKey()), serializedValues.get(scoped.getKey()), ttl, scoped, error);
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.FAILED,
                                finalizeResult.getStatus() == WriteResult.WriteStatus.STORE_UPDATED_CACHE_REPAIR_SCHEDULED,
                                finalizeResult.getStatus());
                    }
                }
            } else {
                for (ConsistencyContext scoped : pending) {
                    if (debugStates.get(scoped.getKey()).finalStatus != null) {
                        continue;
                    }
                    observer.onStoreWrite(scoped.getKey());
                    byte[] serializedValue = serializer.serialize(valuesByKey.get(scoped.getKey()));
                    boolean staged = redisAccessor.stageCacheValue(protocolLeaseKey(scoped.getKey()),
                            tokens.get(scoped.getKey()).getBytes(StandardCharsets.UTF_8),
                            cacheKey(scoped.getKey()), serializedValue, ttl);
                    if (!staged) {
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.FAILED,
                                BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                                false,
                                WriteResult.WriteStatus.STORE_UPDATED);
                        continue;
                    }
                    debugStates.get(scoped.getKey()).stageStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                    try {
                        redisAccessor.finalizeWrite(statusKey(scoped.getKey()), protocolLeaseKey(scoped.getKey()),
                                validityKey(scoped.getKey()), cacheKey(scoped.getKey()),
                                tokens.get(scoped.getKey()).getBytes(StandardCharsets.UTF_8),
                                Duration.ofSeconds(settings.getWriteLockTtlSeconds()), ttl);
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                false,
                                WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED);
                    } catch (RuntimeException error) {
                        WriteResult finalizeResult = handleSetFinalizeFailure(
                                scoped.getKey(), cacheKey(scoped.getKey()), serializedValue, ttl, scoped, error);
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.FAILED,
                                finalizeResult.getStatus() == WriteResult.WriteStatus.STORE_UPDATED_CACHE_REPAIR_SCHEDULED,
                                finalizeResult.getStatus());
                    }
                }
            }
            return toSnapshots(debugStates);
        } finally {
            for (Map.Entry<String, String> tokenEntry : tokens.entrySet()) {
                releaseLock(protocolLeaseKey(tokenEntry.getKey()), tokenEntry.getValue());
            }
        }
    }

    private Map<String, BatchWriteDebugSnapshot> deleteAllOptimized(List<BatchDeleteCommand> keys,
                                                                    ConsistencyContext defaultContext,
                                                                    BatchPersistentOperation<T> batchPersistentOperation) {
        List<ConsistencyContext> pending = new ArrayList<ConsistencyContext>();
        Map<String, String> tokens = new HashMap<String, String>();
        Map<String, BatchWriteDebugState> debugStates = new LinkedHashMap<String, BatchWriteDebugState>();
        BatchRedisAccessor batchRedisAccessor = redisAccessor instanceof BatchRedisAccessor
                ? (BatchRedisAccessor) redisAccessor
                : null;
        List<BatchRedisAccessor.BeginWriteRequest> beginRequests = new ArrayList<BatchRedisAccessor.BeginWriteRequest>();
        for (BatchDeleteCommand command : keys) {
            String key = command.getKey();
            requireKey(key);
            ConsistencyContext merged = ConsistencyContext.merge(defaultContext, command.getContext());
            ConsistencyContext scoped = scope(merged, key, null, CommandType.DELETE, null);
            String token = UUID.randomUUID().toString();
            debugStates.put(key, new BatchWriteDebugState(key, CommandType.DELETE, true));
            debugStates.get(key).stageStatus = BatchWriteDebugSnapshot.StepStatus.SKIPPED;
            pending.add(scoped);
            tokens.put(key, token);
            if (batchRedisAccessor != null) {
                beginRequests.add(new BatchRedisAccessor.BeginWriteRequest(
                        key,
                        statusKey(key),
                        protocolLeaseKey(key),
                        cacheKey(key),
                        validityKey(key),
                        token.getBytes(StandardCharsets.UTF_8),
                        Duration.ofSeconds(settings.getWriteLockTtlSeconds())
                ));
            } else {
                boolean locked = redisAccessor.beginWrite(statusKey(key), protocolLeaseKey(key), cacheKey(key), validityKey(key),
                        token.getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(settings.getWriteLockTtlSeconds()));
                if (!locked) {
                    markFinal(debugStates.get(key),
                            BatchWriteDebugSnapshot.StepStatus.FAILED,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            false,
                            WriteResult.WriteStatus.WRITE_LOCK_BUSY);
                } else {
                    debugStates.get(key).prepareStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                }
            }
        }
        if (batchRedisAccessor != null) {
            Map<String, Boolean> lockedResults = batchRedisAccessor.beginWriteAll(beginRequests);
            List<ConsistencyContext> lockedPending = new ArrayList<ConsistencyContext>();
            for (ConsistencyContext scoped : pending) {
                if (Boolean.TRUE.equals(lockedResults.get(scoped.getKey()))) {
                    debugStates.get(scoped.getKey()).prepareStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                    lockedPending.add(scoped);
                } else {
                    markFinal(debugStates.get(scoped.getKey()),
                            BatchWriteDebugSnapshot.StepStatus.FAILED,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                            BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                            false,
                            WriteResult.WriteStatus.WRITE_LOCK_BUSY);
                }
            }
            pending = lockedPending;
        }
        if (pending.isEmpty()) {
            return toSnapshots(debugStates);
        }
        try {
            executeBatchWriteTransaction(pending, batchPersistentOperation, false, debugStates, tokens, defaultContext);
            if (batchRedisAccessor != null) {
                List<BatchRedisAccessor.FinalizeWriteRequest> finalizeRequests = new ArrayList<BatchRedisAccessor.FinalizeWriteRequest>();
                for (ConsistencyContext scoped : pending) {
                    if (debugStates.get(scoped.getKey()).finalStatus != null) {
                        continue;
                    }
                    observer.onStoreDelete(scoped.getKey());
                    finalizeRequests.add(new BatchRedisAccessor.FinalizeWriteRequest(
                            scoped.getKey(),
                            statusKey(scoped.getKey()),
                            protocolLeaseKey(scoped.getKey()),
                            validityKey(scoped.getKey()),
                            cacheKey(scoped.getKey()),
                            tokens.get(scoped.getKey()).getBytes(StandardCharsets.UTF_8),
                            Duration.ofSeconds(settings.getWriteLockTtlSeconds()),
                            Duration.ofSeconds(1)
                    ));
                }
                try {
                    Map<String, Boolean> finalizeResults = batchRedisAccessor.finalizeWriteAll(finalizeRequests);
                    for (ConsistencyContext scoped : pending) {
                        if (debugStates.get(scoped.getKey()).finalStatus != null) {
                            continue;
                        }
                        if (Boolean.TRUE.equals(finalizeResults.get(scoped.getKey()))) {
                            redisAccessor.delete(validityKey(scoped.getKey()));
                            markFinal(debugStates.get(scoped.getKey()),
                                    debugStates.get(scoped.getKey()).prepareStatus,
                                    debugStates.get(scoped.getKey()).versionStatus,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    false,
                                    WriteResult.WriteStatus.DELETED);
                        } else {
                            markFinal(debugStates.get(scoped.getKey()),
                                    debugStates.get(scoped.getKey()).prepareStatus,
                                    debugStates.get(scoped.getKey()).versionStatus,
                                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                    BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                                    BatchWriteDebugSnapshot.StepStatus.FAILED,
                                    false,
                                    WriteResult.WriteStatus.DELETED);
                        }
                    }
                } catch (RuntimeException error) {
                    for (ConsistencyContext scoped : pending) {
                        if (debugStates.get(scoped.getKey()).finalStatus != null) {
                            continue;
                        }
                        WriteResult finalizeResult = handleDeleteFinalizeFailure(
                                scoped.getKey(), cacheKey(scoped.getKey()), scoped, error);
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                                BatchWriteDebugSnapshot.StepStatus.FAILED,
                                finalizeResult.getStatus() == WriteResult.WriteStatus.DELETED_CACHE_REPAIR_SCHEDULED,
                                finalizeResult.getStatus());
                    }
                }
            } else {
                for (ConsistencyContext scoped : pending) {
                    if (debugStates.get(scoped.getKey()).finalStatus != null) {
                        continue;
                    }
                    observer.onStoreDelete(scoped.getKey());
                    try {
                        redisAccessor.finalizeWrite(statusKey(scoped.getKey()), protocolLeaseKey(scoped.getKey()),
                                validityKey(scoped.getKey()), cacheKey(scoped.getKey()),
                                tokens.get(scoped.getKey()).getBytes(StandardCharsets.UTF_8),
                                Duration.ofSeconds(settings.getWriteLockTtlSeconds()), Duration.ofSeconds(1));
                        redisAccessor.delete(validityKey(scoped.getKey()));
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                false,
                                WriteResult.WriteStatus.DELETED);
                    } catch (RuntimeException error) {
                        WriteResult finalizeResult = handleDeleteFinalizeFailure(
                                scoped.getKey(), cacheKey(scoped.getKey()), scoped, error);
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                                BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                                BatchWriteDebugSnapshot.StepStatus.FAILED,
                                finalizeResult.getStatus() == WriteResult.WriteStatus.DELETED_CACHE_REPAIR_SCHEDULED,
                                finalizeResult.getStatus());
                    }
                }
            }
            return toSnapshots(debugStates);
        } finally {
            for (Map.Entry<String, String> tokenEntry : tokens.entrySet()) {
                releaseLock(protocolLeaseKey(tokenEntry.getKey()), tokenEntry.getValue());
            }
        }
    }

    private void executeBatchWriteTransaction(List<ConsistencyContext> contexts,
                                              BatchPersistentOperation<T> batchPersistentOperation,
                                              boolean update,
                                              Map<String, BatchWriteDebugState> debugStates,
                                              Map<String, String> tokens,
                                              ConsistencyContext transactionContext) {
        ConsistencyContext effectiveTransactionContext = transactionContext == null
                ? contexts.get(0)
                : transactionContext.copy();
        executeWithinTransaction(effectiveTransactionContext, new TransactionAction<Void>() {
            @Override
            public Void execute() {
                Map<String, PersistentOperation.OperationResult<String>> versions = verifyExpectedVersionsBatch(contexts, batchPersistentOperation);
                for (ConsistencyContext scoped : contexts) {
                    PersistentOperation.OperationResult<String> version = versions.get(scoped.getKey());
                    if (version != null && version.getStatus() == PersistentOperation.OperationStatus.VERSION_MISSING) {
                        observer.onVersionRejected(scoped.getKey());
                        rollbackProtocolState(scoped.getKey(), tokens.get(scoped.getKey()));
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                BatchWriteDebugSnapshot.StepStatus.REJECTED,
                                BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                                debugStates.get(scoped.getKey()).stageStatus,
                                BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                                false,
                                WriteResult.WriteStatus.VERSION_REJECTED);
                    } else if (version != null && version.getStatus() == PersistentOperation.OperationStatus.VERSION_REJECTED) {
                        observer.onVersionRejected(scoped.getKey());
                        rollbackProtocolState(scoped.getKey(), tokens.get(scoped.getKey()));
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                BatchWriteDebugSnapshot.StepStatus.REJECTED,
                                BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                                debugStates.get(scoped.getKey()).stageStatus,
                                BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                                false,
                                WriteResult.WriteStatus.VERSION_REJECTED);
                    } else {
                        debugStates.get(scoped.getKey()).versionStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                    }
                }
                List<ConsistencyContext> writable = new ArrayList<ConsistencyContext>();
                for (ConsistencyContext scoped : contexts) {
                    if (debugStates.get(scoped.getKey()).finalStatus == null) {
                        writable.add(scoped);
                    }
                }
                Map<String, PersistentOperation.OperationResult<Void>> batchResult =
                        update ? batchPersistentOperation.updateAll(writable) : batchPersistentOperation.deleteAll(writable);
                for (ConsistencyContext scoped : writable) {
                    PersistentOperation.OperationResult<Void> operationResult = batchResult.get(scoped.getKey());
                    if (operationResult == null) {
                        throw new IllegalStateException("Persistent store batch write result missing");
                    }
                    if (operationResult.getStatus() == PersistentOperation.OperationStatus.VERSION_REJECTED) {
                        observer.onVersionRejected(scoped.getKey());
                        markFinal(debugStates.get(scoped.getKey()),
                                debugStates.get(scoped.getKey()).prepareStatus,
                                debugStates.get(scoped.getKey()).versionStatus,
                                BatchWriteDebugSnapshot.StepStatus.REJECTED,
                                debugStates.get(scoped.getKey()).stageStatus,
                                BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                                false,
                                WriteResult.WriteStatus.VERSION_REJECTED);
                    } else if (operationResult.getStatus() != PersistentOperation.OperationStatus.SUCCESS) {
                        rollbackProtocolState(scoped.getKey(), tokens.get(scoped.getKey()));
                        throw new IllegalStateException("Persistent store batch write failed");
                    } else {
                        debugStates.get(scoped.getKey()).storeStatus = BatchWriteDebugSnapshot.StepStatus.SUCCESS;
                    }
                }
                return null;
            }
        });
    }

    private Map<String, PersistentOperation.OperationResult<String>> verifyExpectedVersionsBatch(List<ConsistencyContext> contexts,
                                                                                                 BatchPersistentOperation<T> batchPersistentOperation) {
        List<ConsistencyContext> versioned = new ArrayList<ConsistencyContext>();
        for (ConsistencyContext scoped : contexts) {
            if (scoped.getVersion() != null && !scoped.getVersion().trim().isEmpty()) {
                versioned.add(scoped);
            }
        }
        Map<String, PersistentOperation.OperationResult<String>> result = new HashMap<String, PersistentOperation.OperationResult<String>>();
        if (versioned.isEmpty()) {
            return result;
        }
        Map<String, PersistentOperation.OperationResult<String>> versionResults = batchPersistentOperation.queryVersionAll(versioned);
        for (ConsistencyContext scoped : versioned) {
            PersistentOperation.OperationResult<String> versionResult = versionResults.get(scoped.getKey());
            if (versionResult == null) {
                throw new IllegalStateException("Persistent store batch version result missing");
            }
            if (versionResult.getStatus() == PersistentOperation.OperationStatus.VERSION_MISSING) {
                result.put(scoped.getKey(), versionResult);
                continue;
            }
            if (versionResult.getStatus() != PersistentOperation.OperationStatus.SUCCESS) {
                throw new IllegalStateException("Persistent store batch version query failed");
            }
            if (!scoped.getVersion().equals(versionResult.getData())) {
                result.put(scoped.getKey(), PersistentOperation.OperationResult.<String>versionRejected());
            }
        }
        return result;
    }

    private List<String> cacheKeys(Collection<String> keys) {
        List<String> result = new ArrayList<String>(keys.size());
        for (String key : keys) {
            result.add(cacheKey(key));
        }
        return result;
    }

    private List<String> validityKeys(Collection<String> keys) {
        List<String> result = new ArrayList<String>(keys.size());
        for (String key : keys) {
            result.add(validityKey(key));
        }
        return result;
    }

    private List<String> statusKeys(Collection<String> keys) {
        List<String> result = new ArrayList<String>(keys.size());
        for (String key : keys) {
            result.add(statusKey(key));
        }
        return result;
    }

    private List<BatchRedisAccessor.ReadProtocolRequest> readProtocolRequests(Collection<String> keys) {
        List<BatchRedisAccessor.ReadProtocolRequest> result = new ArrayList<BatchRedisAccessor.ReadProtocolRequest>(keys.size());
        for (String key : keys) {
            result.add(new BatchRedisAccessor.ReadProtocolRequest(
                    key,
                    cacheKey(key),
                    statusKey(key),
                    validityKey(key)
            ));
        }
        return result;
    }

    private String decodeState(byte[] statusBytes) {
        if (statusBytes == null) {
            return null;
        }
        ProtocolState state = ProtocolState.fromValue(new String(statusBytes, StandardCharsets.UTF_8));
        return state == null ? null : state.value();
    }

    private Map<String, WriteResult> toWriteResults(Map<String, BatchWriteDebugSnapshot> snapshots) {
        Map<String, WriteResult> result = new LinkedHashMap<String, WriteResult>(snapshots.size());
        for (Map.Entry<String, BatchWriteDebugSnapshot> entry : snapshots.entrySet()) {
            result.put(entry.getKey(), WriteResult.of(entry.getValue().getFinalStatus()));
        }
        return result;
    }

    private Map<String, BatchWriteDebugSnapshot> toSnapshots(Map<String, BatchWriteDebugState> states) {
        Map<String, BatchWriteDebugSnapshot> result = new LinkedHashMap<String, BatchWriteDebugSnapshot>(states.size());
        for (Map.Entry<String, BatchWriteDebugState> entry : states.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toSnapshot());
        }
        return result;
    }

    private BatchWriteDebugSnapshot debugSnapshotForSetResult(String key,
                                                              WriteResult.WriteStatus status,
                                                              boolean optimized) {
        BatchWriteDebugState state = new BatchWriteDebugState(key, CommandType.SET, optimized);
        if (status == WriteResult.WriteStatus.WRITE_LOCK_BUSY) {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.FAILED,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    false,
                    status);
        } else if (status == WriteResult.WriteStatus.VERSION_REJECTED) {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.REJECTED,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    false,
                    status);
        } else if (status == WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED) {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    false,
                    status);
        } else if (status == WriteResult.WriteStatus.STORE_UPDATED) {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.FAILED,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    false,
                    status);
        } else {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.FAILED,
                    true,
                    status);
        }
        return state.toSnapshot();
    }

    private BatchWriteDebugSnapshot debugSnapshotForDeleteResult(String key,
                                                                 WriteResult.WriteStatus status,
                                                                 boolean optimized) {
        BatchWriteDebugState state = new BatchWriteDebugState(key, CommandType.DELETE, optimized);
        state.stageStatus = BatchWriteDebugSnapshot.StepStatus.SKIPPED;
        if (status == WriteResult.WriteStatus.WRITE_LOCK_BUSY) {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.FAILED,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    false,
                    status);
        } else if (status == WriteResult.WriteStatus.VERSION_REJECTED) {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.REJECTED,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                    BatchWriteDebugSnapshot.StepStatus.NOT_RUN,
                    false,
                    status);
        } else if (status == WriteResult.WriteStatus.DELETED) {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    false,
                    status);
        } else {
            markFinal(state,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                    BatchWriteDebugSnapshot.StepStatus.SKIPPED,
                    BatchWriteDebugSnapshot.StepStatus.FAILED,
                    true,
                    status);
        }
        return state.toSnapshot();
    }

    private void markFinal(BatchWriteDebugState state,
                           BatchWriteDebugSnapshot.StepStatus prepareStatus,
                           BatchWriteDebugSnapshot.StepStatus versionStatus,
                           BatchWriteDebugSnapshot.StepStatus storeStatus,
                           BatchWriteDebugSnapshot.StepStatus stageStatus,
                           BatchWriteDebugSnapshot.StepStatus finalizeStatus,
                           boolean compensationScheduled,
                           WriteResult.WriteStatus finalStatus) {
        state.prepareStatus = prepareStatus;
        state.versionStatus = versionStatus;
        state.storeStatus = storeStatus;
        state.stageStatus = stageStatus;
        state.finalizeStatus = finalizeStatus;
        state.compensationScheduled = compensationScheduled;
        state.finalStatus = finalStatus;
    }

    private interface TransactionAction<R> {
        R execute();
    }

    @Override
    public void shutdown() {
        if (batchExecutorService != null) {
            batchExecutorService.shutdown();
        }
    }

    private interface BatchAction<I, R> {
        R execute(I item, ConsistencyContext scopedContext);

        default String keyOf(I item) {
            return String.valueOf(item);
        }
    }

    private interface BatchContextResolver<I> {
        ConsistencyContext resolve(I item, ConsistencyContext batchContext);
    }

    private static final class BatchWriteDebugState {
        private final String key;
        private final CommandType commandType;
        private final boolean optimizedPath;
        private BatchWriteDebugSnapshot.StepStatus prepareStatus = BatchWriteDebugSnapshot.StepStatus.NOT_RUN;
        private BatchWriteDebugSnapshot.StepStatus versionStatus = BatchWriteDebugSnapshot.StepStatus.NOT_RUN;
        private BatchWriteDebugSnapshot.StepStatus storeStatus = BatchWriteDebugSnapshot.StepStatus.NOT_RUN;
        private BatchWriteDebugSnapshot.StepStatus stageStatus = BatchWriteDebugSnapshot.StepStatus.NOT_RUN;
        private BatchWriteDebugSnapshot.StepStatus finalizeStatus = BatchWriteDebugSnapshot.StepStatus.NOT_RUN;
        private boolean compensationScheduled;
        private WriteResult.WriteStatus finalStatus;

        private BatchWriteDebugState(String key, CommandType commandType, boolean optimizedPath) {
            this.key = key;
            this.commandType = commandType;
            this.optimizedPath = optimizedPath;
        }

        private BatchWriteDebugSnapshot toSnapshot() {
            return new BatchWriteDebugSnapshot(
                    key,
                    commandType,
                    optimizedPath,
                    prepareStatus,
                    versionStatus,
                    storeStatus,
                    stageStatus,
                    finalizeStatus,
                    compensationScheduled,
                    finalStatus);
        }
    }

    private void requireKey(String key) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }

    private void requireTtl(Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be greater than zero");
        }
    }
}
