package io.github.cacheconsistency.core;

import io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * Protocol-oriented client for cache and persistent-store consistency.
 *
 * <p>The client exposes single-key operations as the primary consistency unit and
 * offers batch helpers that still execute the single-key protocol per entry.
 * Batch execution can be parallelized through settings, but it is not a multi-key atomic protocol.</p>
 */
public interface ConsistencyClient<T> {
    /**
     * Reads a value using the protocol read path.
     */
    ReadResult<T> get(String key, Duration ttl, ConsistencyContext context);

    /**
     * Writes a value using the protocol write path.
     */
    WriteResult set(String key, T value, Duration ttl, ConsistencyContext context);

    /**
     * Deletes a value using the protocol write path.
     */
    WriteResult delete(String key, ConsistencyContext context);

    /**
     * Reads a collection of keys by executing the single-key protocol for each key.
     */
    Map<String, ReadResult<T>> getAll(Collection<String> keys, Duration ttl, ConsistencyContext context);

    /**
     * Writes a collection of key/value pairs by executing the single-key protocol for each key.
     */
    Map<String, WriteResult> setAll(Map<String, T> values, Duration ttl, ConsistencyContext context);

    /**
     * Writes a collection of batch items, each with its own consistency context.
     */
    Map<String, WriteResult> setAllWithContexts(Collection<BatchSetCommand<T>> commands,
                                                Duration ttl,
                                                ConsistencyContext defaultContext);

    /**
     * Writes a collection of batch items and returns per-key protocol step details.
     */
    Map<String, BatchWriteDebugSnapshot> setAllWithContextsDetailed(Collection<BatchSetCommand<T>> commands,
                                                                    Duration ttl,
                                                                    ConsistencyContext defaultContext);

    /**
     * Deletes a collection of keys by executing the single-key protocol for each key.
     */
    Map<String, WriteResult> deleteAll(Collection<String> keys, ConsistencyContext context);

    /**
     * Deletes a collection of batch items, each with its own consistency context.
     */
    Map<String, WriteResult> deleteAllWithContexts(Collection<BatchDeleteCommand> commands,
                                                   ConsistencyContext defaultContext);

    /**
     * Deletes a collection of batch items and returns per-key protocol step details.
     */
    Map<String, BatchWriteDebugSnapshot> deleteAllWithContextsDetailed(Collection<BatchDeleteCommand> commands,
                                                                       ConsistencyContext defaultContext);

    default void shutdown() {
    }
}
