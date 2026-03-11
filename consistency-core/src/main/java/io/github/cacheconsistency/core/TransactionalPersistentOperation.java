package io.github.cacheconsistency.core;

/**
 * Extension of {@link PersistentOperation} for stores that can wrap version query and write
 * in a single transaction boundary.
 */
public interface TransactionalPersistentOperation<T> extends PersistentOperation<T> {
    /**
     * Executes the callback inside the store's transaction mechanism.
     */
    <R> R executeInTransaction(ConsistencyContext context, TransactionCallback<R> callback);

    /**
     * Callback executed inside the store-managed transaction.
     */
    interface TransactionCallback<R> {
        R doInTransaction();
    }
}
