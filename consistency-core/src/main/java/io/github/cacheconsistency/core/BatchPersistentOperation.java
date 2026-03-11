package io.github.cacheconsistency.core;

import java.util.Collection;
import java.util.Map;

/**
 * Optional batch extension for persistent-store adapters.
 *
 * <p>Implement this interface when the backing store can efficiently execute
 * multi-key reads, version queries, updates, or deletes.</p>
 */
public interface BatchPersistentOperation<T> extends PersistentOperation<T> {
    Map<String, OperationResult<T>> readAll(Collection<ConsistencyContext> contexts);

    Map<String, OperationResult<String>> queryVersionAll(Collection<ConsistencyContext> contexts);

    Map<String, OperationResult<Void>> updateAll(Collection<ConsistencyContext> contexts);

    Map<String, OperationResult<Void>> deleteAll(Collection<ConsistencyContext> contexts);
}
