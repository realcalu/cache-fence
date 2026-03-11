package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.VersionHealingOperation;
import io.github.cacheconsistency.core.support.AsyncGhostWriteHealer;
import io.github.cacheconsistency.core.support.GhostWriteHealer;

public class InMemoryGhostWriteHealer implements GhostWriteHealer {
    private final GhostWriteHealer delegate;
    private volatile long delayMillis;

    public InMemoryGhostWriteHealer(InMemoryPersistentOperation<?> operation) {
        this.delegate = new AsyncGhostWriteHealer(new VersionHealingOperation() {
            @Override
            public void healVersion(String key, ConsistencyContext context) {
                delayIfNecessary();
                operation.bumpVersion(key);
            }
        });
    }

    @Override
    public void scheduleHeal(String key, ConsistencyContext context) {
        delegate.scheduleHeal(key, context);
    }

    public void setDelayMillis(long delayMillis) {
        this.delayMillis = delayMillis;
    }

    private void delayIfNecessary() {
        if (delayMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
