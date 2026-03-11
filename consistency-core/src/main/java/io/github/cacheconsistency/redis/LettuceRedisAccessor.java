package io.github.cacheconsistency.redis;

import io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot;
import io.github.cacheconsistency.core.support.BatchRedisAccessor;
import io.github.cacheconsistency.core.support.RedisAccessor;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.time.Duration;

/**
 * Lettuce-based Redis accessor that implements the protocol with Lua scripts where atomicity matters.
 */
public class LettuceRedisAccessor implements BatchRedisAccessor {
    private static final byte[] BEGIN_WRITE_SCRIPT =
            ("if redis.call('get', KEYS[1]) == ARGV[5] then return 0 end " +
                    "redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2]); " +
                    "redis.call('set', KEYS[2], ARGV[3], 'EX', ARGV[2]); " +
                    "redis.call('del', KEYS[3]); " +
                    "redis.call('set', KEYS[4], ARGV[4], 'EX', ARGV[2]); " +
                    "return 1 end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] COMPARE_AND_DELETE_SCRIPT =
            ("if redis.call('get', KEYS[1]) == ARGV[1] then " +
                    "return redis.call('del', KEYS[1]) else return 0 end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] WRITE_CACHE_AND_RELEASE_LOCK_SCRIPT =
            ("if redis.call('get', KEYS[1]) == ARGV[1] then " +
                    "redis.call('set', KEYS[2], ARGV[2], 'EX', ARGV[3]); " +
                    "redis.call('del', KEYS[1]); return 1 else return 0 end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] STAGE_CACHE_VALUE_SCRIPT =
            ("if redis.call('get', KEYS[1]) == ARGV[1] then " +
                    "redis.call('set', KEYS[2], ARGV[2], 'EX', ARGV[3]); return 1 else return 0 end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] DELETE_CACHE_AND_RELEASE_LOCK_SCRIPT =
            ("if redis.call('get', KEYS[1]) == ARGV[1] then " +
                    "redis.call('del', KEYS[2]); redis.call('del', KEYS[1]); " +
                    "return 1 else return 0 end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] FINALIZE_WRITE_SCRIPT =
            ("if redis.call('get', KEYS[2]) == ARGV[1] then " +
                    "redis.call('set', KEYS[1], ARGV[2], 'EX', ARGV[3]); " +
                    "if redis.call('exists', KEYS[4]) == 1 then " +
                    "redis.call('set', KEYS[3], ARGV[4], 'EX', ARGV[5]); end; " +
                    "redis.call('del', KEYS[2]); return 1 else return 0 end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] ROLLBACK_WRITE_SCRIPT =
            ("if redis.call('get', KEYS[2]) == ARGV[1] then " +
                    "redis.call('del', KEYS[1]); redis.call('del', KEYS[2]); redis.call('del', KEYS[3]); " +
                    "return 1 else return 0 end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private static final byte[] READ_PROTOCOL_SCRIPT =
            ("local data = redis.call('get', KEYS[1]); " +
                    "if data then " +
                    "  local validity = redis.call('get', KEYS[3]); " +
                    "  if validity == ARGV[1] then return {ARGV[3]} end; " +
                    "  return {ARGV[2], data}; " +
                    "end; " +
                    "local status = redis.call('get', KEYS[2]); " +
                    "if not status then return {ARGV[4]} end; " +
                    "if status == ARGV[5] then return {ARGV[5]} end; " +
                    "return {ARGV[6]} end")
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisCommands<byte[], byte[]> commands;

    public LettuceRedisAccessor(StatefulRedisConnection<byte[], byte[]> connection) {
        this.connection = connection;
        this.commands = connection.sync();
    }

    public static StatefulRedisConnection<byte[], byte[]> connect(io.lettuce.core.RedisClient client) {
        return client.connect(ByteArrayCodec.INSTANCE);
    }

    @Override
    public byte[] get(String key) {
        return commands.get(raw(key));
    }

    @Override
    public Map<String, byte[]> getAll(Collection<String> keys) {
        byte[][] rawKeys = new byte[keys.size()][];
        String[] orderedKeys = new String[keys.size()];
        int index = 0;
        for (String key : keys) {
            orderedKeys[index] = key;
            rawKeys[index] = raw(key);
            index++;
        }
        List<KeyValue<byte[], byte[]>> values = commands.mget(rawKeys);
        Map<String, byte[]> result = new LinkedHashMap<String, byte[]>(orderedKeys.length);
        for (int i = 0; i < orderedKeys.length; i++) {
            KeyValue<byte[], byte[]> value = values.get(i);
            result.put(orderedKeys[i], value == null || !value.hasValue() ? null : value.getValue());
        }
        return result;
    }

    @Override
    public void set(String key, byte[] value, Duration ttl) {
        commands.set(raw(key), value, SetArgs.Builder.ex(ttl.getSeconds()));
    }

    @Override
    public void setAll(Map<String, byte[]> values, Duration ttl) {
        for (Map.Entry<String, byte[]> entry : values.entrySet()) {
            set(entry.getKey(), entry.getValue(), ttl);
        }
    }

    @Override
    public Map<String, Boolean> beginWriteAll(Collection<BeginWriteRequest> requests) {
        List<RedisFuture<Long>> futures = new ArrayList<RedisFuture<Long>>();
        List<String> keys = new ArrayList<String>();
        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        connection.setAutoFlushCommands(false);
        try {
            for (BeginWriteRequest request : requests) {
                keys.add(request.getKey());
                futures.add(async.eval(BEGIN_WRITE_SCRIPT, ScriptOutputType.INTEGER,
                        new byte[][]{
                                raw(request.getStatusKey()),
                                raw(request.getProtocolLeaseKey()),
                                raw(request.getDataKey()),
                                raw(request.getValidityKey())
                        },
                        raw(ProtocolState.IN_WRITING.value()),
                        raw(String.valueOf(request.getWriteWindowTtl().getSeconds())),
                        request.getLeaseToken(),
                        raw(ProtocolValidity.INVALID.value()),
                        raw(ProtocolState.IN_WRITING.value())));
            }
            connection.flushCommands();
            return collectBooleanResults(keys, futures);
        } finally {
            connection.setAutoFlushCommands(true);
        }
    }

    @Override
    public Map<String, Boolean> stageCacheValueAll(Collection<StageCacheValueRequest> requests) {
        List<RedisFuture<Long>> futures = new ArrayList<RedisFuture<Long>>();
        List<String> keys = new ArrayList<String>();
        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        connection.setAutoFlushCommands(false);
        try {
            for (StageCacheValueRequest request : requests) {
                keys.add(request.getKey());
                futures.add(async.eval(STAGE_CACHE_VALUE_SCRIPT, ScriptOutputType.INTEGER,
                        new byte[][]{raw(request.getProtocolLeaseKey()), raw(request.getCacheKey())},
                        request.getExpectedValue(),
                        request.getCacheValue(),
                        raw(String.valueOf(request.getTtl().getSeconds()))));
            }
            connection.flushCommands();
            return collectBooleanResults(keys, futures);
        } finally {
            connection.setAutoFlushCommands(true);
        }
    }

    @Override
    public Map<String, Boolean> finalizeWriteAll(Collection<FinalizeWriteRequest> requests) {
        List<RedisFuture<Long>> futures = new ArrayList<RedisFuture<Long>>();
        List<String> keys = new ArrayList<String>();
        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        connection.setAutoFlushCommands(false);
        try {
            for (FinalizeWriteRequest request : requests) {
                keys.add(request.getKey());
                futures.add(async.eval(FINALIZE_WRITE_SCRIPT, ScriptOutputType.INTEGER,
                        new byte[][]{
                                raw(request.getStatusKey()),
                                raw(request.getProtocolLeaseKey()),
                                raw(request.getValidityKey()),
                                raw(request.getDataKey())
                        },
                        request.getLeaseToken(),
                        raw(ProtocolState.LAST_WRITE_SUCCESS.value()),
                        raw(String.valueOf(request.getStatusTtl().getSeconds())),
                        raw(ProtocolValidity.VALID.value()),
                        raw(String.valueOf(request.getDataTtl().getSeconds()))));
            }
            connection.flushCommands();
            return collectBooleanResults(keys, futures);
        } finally {
            connection.setAutoFlushCommands(true);
        }
    }

    @Override
    public Map<String, ReadProtocolResult> readProtocolAll(Collection<ReadProtocolRequest> requests) {
        List<RedisFuture<Object>> futures = new ArrayList<RedisFuture<Object>>();
        List<ReadProtocolRequest> requestList = new ArrayList<ReadProtocolRequest>();
        RedisAsyncCommands<byte[], byte[]> async = connection.async();
        connection.setAutoFlushCommands(false);
        try {
            for (ReadProtocolRequest request : requests) {
                requestList.add(request);
                futures.add(async.eval(READ_PROTOCOL_SCRIPT, ScriptOutputType.MULTI,
                        new byte[][]{raw(request.getDataKey()), raw(request.getStatusKey()), raw(request.getValidityKey())},
                        raw(ProtocolValidity.INVALID.value()),
                        raw("CACHE_HIT"),
                        raw("INVALID_CACHE"),
                        raw("STATUS_MISSING"),
                        raw(ProtocolState.IN_WRITING.value()),
                        raw(ProtocolState.LAST_WRITE_SUCCESS.value())));
            }
            connection.flushCommands();
            Map<String, ReadProtocolResult> result = new LinkedHashMap<String, ReadProtocolResult>(requestList.size());
            for (int i = 0; i < requestList.size(); i++) {
                result.put(requestList.get(i).getKey(), toReadProtocolResult(requestList.get(i), futures.get(i)));
            }
            return result;
        } finally {
            connection.setAutoFlushCommands(true);
        }
    }

    @Override
    public boolean setIfAbsent(String key, byte[] value, Duration ttl) {
        String result = commands.set(raw(key), value, SetArgs.Builder.nx().ex(ttl.getSeconds()));
        return "OK".equalsIgnoreCase(result);
    }

    @Override
    public boolean compareAndDelete(String key, byte[] expectedValue) {
        Long deleted = commands.eval(COMPARE_AND_DELETE_SCRIPT, ScriptOutputType.INTEGER,
                new byte[][]{raw(key)}, expectedValue);
        return deleted != null && deleted.longValue() > 0L;
    }

    @Override
    public boolean beginWrite(String statusKey,
                              String protocolLeaseKey,
                              String dataKey,
                              String validityKey,
                              byte[] leaseToken,
                              Duration writeWindowTtl) {
        // Prewrite invalidates the old cache and records the lease that is allowed to finalize later.
        Long result = commands.eval(BEGIN_WRITE_SCRIPT, ScriptOutputType.INTEGER,
                new byte[][]{raw(statusKey), raw(protocolLeaseKey), raw(dataKey), raw(validityKey)},
                raw(ProtocolState.IN_WRITING.value()), raw(String.valueOf(writeWindowTtl.getSeconds())),
                leaseToken, raw(ProtocolValidity.INVALID.value()), raw(ProtocolState.IN_WRITING.value()));
        return result != null && result.longValue() > 0L;
    }

    @Override
    public boolean writeCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey, byte[] cacheValue, Duration ttl) {
        Long result = commands.eval(WRITE_CACHE_AND_RELEASE_LOCK_SCRIPT, ScriptOutputType.INTEGER,
                new byte[][]{raw(lockKey), raw(cacheKey)},
                expectedValue, cacheValue, raw(String.valueOf(ttl.getSeconds())));
        return result != null && result.longValue() > 0L;
    }

    @Override
    public boolean stageCacheValue(String protocolLeaseKey,
                                   byte[] expectedValue,
                                   String cacheKey,
                                   byte[] cacheValue,
                                   Duration ttl) {
        Long result = commands.eval(STAGE_CACHE_VALUE_SCRIPT, ScriptOutputType.INTEGER,
                new byte[][]{raw(protocolLeaseKey), raw(cacheKey)},
                expectedValue, cacheValue, raw(String.valueOf(ttl.getSeconds())));
        return result != null && result.longValue() > 0L;
    }

    @Override
    public boolean deleteCacheAndReleaseLock(String lockKey, byte[] expectedValue, String cacheKey) {
        Long result = commands.eval(DELETE_CACHE_AND_RELEASE_LOCK_SCRIPT, ScriptOutputType.INTEGER,
                new byte[][]{raw(lockKey), raw(cacheKey)}, expectedValue);
        return result != null && result.longValue() > 0L;
    }

    @Override
    public boolean finalizeWrite(String statusKey,
                                 String protocolLeaseKey,
                                 String validityKey,
                                 String dataKey,
                                 byte[] leaseToken,
                                 Duration statusTtl,
                                 Duration dataTtl) {
        // Finalize publishes the successful write state only if the same writer still owns the lease.
        Long result = commands.eval(FINALIZE_WRITE_SCRIPT, ScriptOutputType.INTEGER,
                new byte[][]{raw(statusKey), raw(protocolLeaseKey), raw(validityKey), raw(dataKey)},
                leaseToken, raw(ProtocolState.LAST_WRITE_SUCCESS.value()), raw(String.valueOf(statusTtl.getSeconds())),
                raw(ProtocolValidity.VALID.value()), raw(String.valueOf(dataTtl.getSeconds())));
        return result != null && result.longValue() > 0L;
    }

    @Override
    public boolean rollbackWrite(String statusKey,
                                 String protocolLeaseKey,
                                 String validityKey,
                                 byte[] leaseToken) {
        Long result = commands.eval(ROLLBACK_WRITE_SCRIPT, ScriptOutputType.INTEGER,
                new byte[][]{raw(statusKey), raw(protocolLeaseKey), raw(validityKey)}, leaseToken);
        return result != null && result.longValue() > 0L;
    }

    @Override
    public long delete(String... keys) {
        byte[][] rawKeys = new byte[keys.length][];
        for (int index = 0; index < keys.length; index++) {
            rawKeys[index] = raw(keys[index]);
        }
        return commands.del(rawKeys);
    }

    @Override
    public void close() {
        connection.close();
    }

    private byte[] raw(String key) {
        return key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private Map<String, Boolean> collectBooleanResults(List<String> keys, List<RedisFuture<Long>> futures) {
        Map<String, Boolean> result = new LinkedHashMap<String, Boolean>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            try {
                Long value = futures.get(i).get();
                result.put(keys.get(i), value != null && value.longValue() > 0L);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Batch Redis pipeline interrupted", interruptedException);
            } catch (ExecutionException executionException) {
                throw new IllegalStateException("Batch Redis pipeline failed", executionException.getCause());
            }
        }
        return result;
    }

    private ReadProtocolResult toReadProtocolResult(ReadProtocolRequest request, RedisFuture<Object> future) {
        try {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) future.get();
            String state = new String((byte[]) values.get(0), java.nio.charset.StandardCharsets.UTF_8);
            if ("CACHE_HIT".equals(state)) {
                return new ReadProtocolResult(
                        new BatchReadDebugSnapshot(
                                request.getKey(),
                                request.getDataKey(),
                                request.getStatusKey(),
                                request.getValidityKey(),
                                true,
                                null,
                                null,
                                BatchReadDebugSnapshot.Decision.CACHE_HIT),
                        (byte[]) values.get(1));
            }
            if ("INVALID_CACHE".equals(state)) {
                return new ReadProtocolResult(
                        new BatchReadDebugSnapshot(
                                request.getKey(),
                                request.getDataKey(),
                                request.getStatusKey(),
                                request.getValidityKey(),
                                true,
                                null,
                                ProtocolValidity.INVALID,
                                BatchReadDebugSnapshot.Decision.INVALID_CACHE),
                        null);
            }
            if (ProtocolState.IN_WRITING.value().equals(state)) {
                return new ReadProtocolResult(
                        new BatchReadDebugSnapshot(
                                request.getKey(),
                                request.getDataKey(),
                                request.getStatusKey(),
                                request.getValidityKey(),
                                false,
                                ProtocolState.IN_WRITING,
                                null,
                                BatchReadDebugSnapshot.Decision.IN_WRITING),
                        null);
            }
            if (ProtocolState.LAST_WRITE_SUCCESS.value().equals(state)) {
                return new ReadProtocolResult(
                        new BatchReadDebugSnapshot(
                                request.getKey(),
                                request.getDataKey(),
                                request.getStatusKey(),
                                request.getValidityKey(),
                                false,
                                ProtocolState.LAST_WRITE_SUCCESS,
                                null,
                                BatchReadDebugSnapshot.Decision.LAST_WRITE_SUCCESS),
                        null);
            }
            return new ReadProtocolResult(
                    new BatchReadDebugSnapshot(
                            request.getKey(),
                            request.getDataKey(),
                            request.getStatusKey(),
                            request.getValidityKey(),
                            false,
                            null,
                            null,
                            BatchReadDebugSnapshot.Decision.STATUS_MISSING),
                    null);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Batch Redis read pipeline interrupted", interruptedException);
        } catch (ExecutionException executionException) {
            throw new IllegalStateException("Batch Redis read pipeline failed", executionException.getCause());
        }
    }
}
