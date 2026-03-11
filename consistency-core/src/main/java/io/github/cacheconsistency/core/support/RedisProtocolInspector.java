package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.protocol.ProtocolSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Protocol inspector backed by Redis key reads.
 */
public final class RedisProtocolInspector implements ProtocolInspector {
    private final RedisAccessor redisAccessor;
    private final ProtocolKeys protocolKeys;

    public RedisProtocolInspector(RedisAccessor redisAccessor, String keyPrefix) {
        this(redisAccessor, new ProtocolKeys(keyPrefix));
    }

    public RedisProtocolInspector(RedisAccessor redisAccessor, ProtocolKeys protocolKeys) {
        this.redisAccessor = redisAccessor;
        this.protocolKeys = protocolKeys;
    }

    @Override
    public ProtocolSnapshot snapshot(String key) {
        byte[] data = redisAccessor.get(protocolKeys.data(key));
        byte[] status = redisAccessor.get(protocolKeys.status(key));
        byte[] validity = redisAccessor.get(protocolKeys.validity(key));
        byte[] readLease = redisAccessor.get(protocolKeys.lease(key));
        byte[] writeLease = redisAccessor.get(protocolKeys.protocolLease(key));

        return new ProtocolSnapshot(
                key,
                data != null,
                parseState(status),
                parseValidity(validity),
                toUtf8(readLease),
                toUtf8(writeLease)
        );
    }

    @Override
    public BatchReadDebugSnapshot explainRead(String key) {
        if (redisAccessor instanceof BatchRedisAccessor) {
            Map<String, BatchReadDebugSnapshot> snapshots =
                    explainReadAll(java.util.Collections.singletonList(key));
            return snapshots.get(key);
        }
        byte[] data = redisAccessor.get(protocolKeys.data(key));
        if (data != null) {
            byte[] validity = redisAccessor.get(protocolKeys.validity(key));
            ProtocolValidity parsedValidity = parseValidity(validity);
            if (parsedValidity == ProtocolValidity.INVALID) {
                return new BatchReadDebugSnapshot(
                        key,
                        protocolKeys.data(key),
                        protocolKeys.status(key),
                        protocolKeys.validity(key),
                        true,
                        null,
                        ProtocolValidity.INVALID,
                        BatchReadDebugSnapshot.Decision.INVALID_CACHE
                );
            }
            return new BatchReadDebugSnapshot(
                    key,
                    protocolKeys.data(key),
                    protocolKeys.status(key),
                    protocolKeys.validity(key),
                    true,
                    null,
                    parsedValidity,
                    BatchReadDebugSnapshot.Decision.CACHE_HIT
            );
        }
        ProtocolState state = parseState(redisAccessor.get(protocolKeys.status(key)));
        if (state == ProtocolState.IN_WRITING) {
            return new BatchReadDebugSnapshot(
                    key,
                    protocolKeys.data(key),
                    protocolKeys.status(key),
                    protocolKeys.validity(key),
                    false,
                    ProtocolState.IN_WRITING,
                    null,
                    BatchReadDebugSnapshot.Decision.IN_WRITING
            );
        }
        if (state == ProtocolState.LAST_WRITE_SUCCESS) {
            return new BatchReadDebugSnapshot(
                    key,
                    protocolKeys.data(key),
                    protocolKeys.status(key),
                    protocolKeys.validity(key),
                    false,
                    ProtocolState.LAST_WRITE_SUCCESS,
                    null,
                    BatchReadDebugSnapshot.Decision.LAST_WRITE_SUCCESS
            );
        }
        return new BatchReadDebugSnapshot(
                key,
                protocolKeys.data(key),
                protocolKeys.status(key),
                protocolKeys.validity(key),
                false,
                null,
                null,
                BatchReadDebugSnapshot.Decision.STATUS_MISSING
        );
    }

    @Override
    public Map<String, ProtocolSnapshot> snapshotAll(Collection<String> keys) {
        if (!(redisAccessor instanceof BatchRedisAccessor)) {
            return ProtocolInspector.super.snapshotAll(keys);
        }
        BatchRedisAccessor batchRedisAccessor = (BatchRedisAccessor) redisAccessor;
        Map<String, byte[]> data = batchRedisAccessor.getAll(protocolDataKeys(keys));
        Map<String, byte[]> status = batchRedisAccessor.getAll(protocolStatusKeys(keys));
        Map<String, byte[]> validity = batchRedisAccessor.getAll(protocolValidityKeys(keys));
        Map<String, byte[]> readLease = batchRedisAccessor.getAll(protocolLeaseKeys(keys));
        Map<String, byte[]> writeLease = batchRedisAccessor.getAll(protocolWriteLeaseKeys(keys));
        Map<String, ProtocolSnapshot> result = new LinkedHashMap<String, ProtocolSnapshot>();
        for (String key : keys) {
            result.put(key, new ProtocolSnapshot(
                    key,
                    data.get(protocolKeys.data(key)) != null,
                    parseState(status.get(protocolKeys.status(key))),
                    parseValidity(validity.get(protocolKeys.validity(key))),
                    toUtf8(readLease.get(protocolKeys.lease(key))),
                    toUtf8(writeLease.get(protocolKeys.protocolLease(key)))
            ));
        }
        return result;
    }

    @Override
    public Map<String, BatchReadDebugSnapshot> explainReadAll(Collection<String> keys) {
        if (!(redisAccessor instanceof BatchRedisAccessor)) {
            return ProtocolInspector.super.explainReadAll(keys);
        }
        BatchRedisAccessor batchRedisAccessor = (BatchRedisAccessor) redisAccessor;
        Map<String, BatchRedisAccessor.ReadProtocolResult> readResults = batchRedisAccessor.readProtocolAll(readProtocolRequests(keys));
        Map<String, BatchReadDebugSnapshot> result = new LinkedHashMap<String, BatchReadDebugSnapshot>();
        for (String key : keys) {
            BatchRedisAccessor.ReadProtocolResult readResult = readResults.get(key);
            if (readResult == null) {
                throw new IllegalStateException("Batch Redis read protocol result missing");
            }
            result.put(key, readResult.getDebugSnapshot());
        }
        return result;
    }

    private ProtocolState parseState(byte[] status) {
        return status == null ? null : ProtocolState.fromValue(toUtf8(status));
    }

    private ProtocolValidity parseValidity(byte[] validity) {
        return validity == null ? null : ProtocolValidity.fromValue(toUtf8(validity));
    }

    private String toUtf8(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private Collection<String> protocolDataKeys(Collection<String> keys) {
        return mapKeys(keys, new KeyMapper() {
            @Override
            public String map(String key) {
                return protocolKeys.data(key);
            }
        });
    }

    private Collection<String> protocolStatusKeys(Collection<String> keys) {
        return mapKeys(keys, new KeyMapper() {
            @Override
            public String map(String key) {
                return protocolKeys.status(key);
            }
        });
    }

    private Collection<String> protocolValidityKeys(Collection<String> keys) {
        return mapKeys(keys, new KeyMapper() {
            @Override
            public String map(String key) {
                return protocolKeys.validity(key);
            }
        });
    }

    private Collection<String> protocolLeaseKeys(Collection<String> keys) {
        return mapKeys(keys, new KeyMapper() {
            @Override
            public String map(String key) {
                return protocolKeys.lease(key);
            }
        });
    }

    private Collection<String> protocolWriteLeaseKeys(Collection<String> keys) {
        return mapKeys(keys, new KeyMapper() {
            @Override
            public String map(String key) {
                return protocolKeys.protocolLease(key);
            }
        });
    }

    private Collection<String> mapKeys(Collection<String> keys, KeyMapper mapper) {
        Collection<String> result = new ArrayList<String>(keys.size());
        for (String key : keys) {
            result.add(mapper.map(key));
        }
        return result;
    }

    private Collection<BatchRedisAccessor.ReadProtocolRequest> readProtocolRequests(Collection<String> keys) {
        Collection<BatchRedisAccessor.ReadProtocolRequest> result =
                new ArrayList<BatchRedisAccessor.ReadProtocolRequest>(keys.size());
        for (String key : keys) {
            result.add(new BatchRedisAccessor.ReadProtocolRequest(
                    key,
                    protocolKeys.data(key),
                    protocolKeys.status(key),
                    protocolKeys.validity(key)
            ));
        }
        return result;
    }

    private interface KeyMapper {
        String map(String key);
    }
}
