package io.github.cacheconsistency.spring;

import io.github.cacheconsistency.core.support.ConsistencyObserver;

import java.util.List;

public final class CompositeConsistencyObserver implements ConsistencyObserver {
    private final List<ConsistencyObserver> observers;

    public CompositeConsistencyObserver(List<ConsistencyObserver> observers) {
        this.observers = observers;
    }

    @Override
    public void onCacheHit(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onCacheHit(key);
        }
    }

    @Override
    public void onCacheMiss(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onCacheMiss(key);
        }
    }

    @Override
    public void onLeaseAcquired(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onLeaseAcquired(key);
        }
    }

    @Override
    public void onStoreRead(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onStoreRead(key);
        }
    }

    @Override
    public void onStoreWrite(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onStoreWrite(key);
        }
    }

    @Override
    public void onStoreDelete(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onStoreDelete(key);
        }
    }

    @Override
    public void onVersionRejected(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onVersionRejected(key);
        }
    }

    @Override
    public void onFinalizeFailure(String key, String action, Throwable error) {
        for (ConsistencyObserver observer : observers) {
            observer.onFinalizeFailure(key, action, error);
        }
    }

    @Override
    public void onGhostWriteHealScheduled(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onGhostWriteHealScheduled(key);
        }
    }

    @Override
    public void onGhostWriteHealSuccess(String key) {
        for (ConsistencyObserver observer : observers) {
            observer.onGhostWriteHealSuccess(key);
        }
    }

    @Override
    public void onGhostWriteHealFailure(String key, Throwable error) {
        for (ConsistencyObserver observer : observers) {
            observer.onGhostWriteHealFailure(key, error);
        }
    }

    @Override
    public void onBatchOperation(String action, int size, boolean optimized) {
        for (ConsistencyObserver observer : observers) {
            observer.onBatchOperation(action, size, optimized);
        }
    }

    @Override
    public void onCompensationScheduled(String key, String action) {
        for (ConsistencyObserver observer : observers) {
            observer.onCompensationScheduled(key, action);
        }
    }

    @Override
    public void onCompensationSuccess(String key, String action, int attempt) {
        for (ConsistencyObserver observer : observers) {
            observer.onCompensationSuccess(key, action, attempt);
        }
    }

    @Override
    public void onCompensationFailure(String key, String action, int attempt, Throwable error) {
        for (ConsistencyObserver observer : observers) {
            observer.onCompensationFailure(key, action, attempt, error);
        }
    }
}
