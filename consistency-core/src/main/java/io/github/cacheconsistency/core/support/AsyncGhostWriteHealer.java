package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.VersionHealingOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncGhostWriteHealer implements GhostWriteHealer {
    private static final Logger log = LoggerFactory.getLogger(AsyncGhostWriteHealer.class);

    private final VersionHealingOperation healingOperation;
    private final ExecutorService executorService;
    private final ConsistencyObserver observer;

    public AsyncGhostWriteHealer(VersionHealingOperation healingOperation) {
        this(healingOperation,
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new NamedThreadFactory("cck-ghost-heal")),
                ConsistencyObserver.NoOpConsistencyObserver.INSTANCE);
    }

    public AsyncGhostWriteHealer(VersionHealingOperation healingOperation, ConsistencyObserver observer) {
        this(healingOperation,
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new NamedThreadFactory("cck-ghost-heal")),
                observer);
    }

    AsyncGhostWriteHealer(VersionHealingOperation healingOperation,
                          ExecutorService executorService,
                          ConsistencyObserver observer) {
        this.healingOperation = healingOperation;
        this.executorService = executorService;
        this.observer = observer;
    }

    @Override
    public void scheduleHeal(final String key, final ConsistencyContext context) {
        observer.onGhostWriteHealScheduled(key);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    healingOperation.healVersion(key, context);
                    observer.onGhostWriteHealSuccess(key);
                } catch (RuntimeException error) {
                    observer.onGhostWriteHealFailure(key, error);
                    log.warn("Ghost write heal failed for key {}", key, error);
                }
            }
        });
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }
}
