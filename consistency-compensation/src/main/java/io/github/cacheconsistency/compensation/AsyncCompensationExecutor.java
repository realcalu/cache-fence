package io.github.cacheconsistency.compensation;

import io.github.cacheconsistency.core.support.ConsistencyObserver;
import io.github.cacheconsistency.core.support.RedisAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled retry executor for optional cache compensation tasks.
 */
public class AsyncCompensationExecutor implements CompensationExecutor {
    private static final Logger log = LoggerFactory.getLogger(AsyncCompensationExecutor.class);

    private final RedisAccessor redisAccessor;
    private final int maxRetries;
    private final long retryDelayMillis;
    private final ScheduledExecutorService executorService;
    private final CompensationTaskStore taskStore;
    private final ConsistencyObserver observer;

    public AsyncCompensationExecutor(RedisAccessor redisAccessor, int maxRetries, long retryDelayMillis) {
        this(redisAccessor, maxRetries, retryDelayMillis,
                new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("cck-compensation")),
                CompensationTaskStore.NoOpCompensationTaskStore.INSTANCE,
                ConsistencyObserver.NoOpConsistencyObserver.INSTANCE);
    }

    public AsyncCompensationExecutor(RedisAccessor redisAccessor,
                                     int maxRetries,
                                     long retryDelayMillis,
                                     CompensationTaskStore taskStore,
                                     ConsistencyObserver observer) {
        this(redisAccessor, maxRetries, retryDelayMillis,
                new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("cck-compensation")),
                taskStore, observer);
    }

    AsyncCompensationExecutor(RedisAccessor redisAccessor,
                              int maxRetries,
                              long retryDelayMillis,
                              ScheduledExecutorService executorService,
                              CompensationTaskStore taskStore,
                              ConsistencyObserver observer) {
        this.redisAccessor = redisAccessor;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
        this.executorService = executorService;
        this.taskStore = taskStore;
        this.observer = observer;
    }

    public void replayPending() {
        List<CompensationTaskStore.CompensationTask> tasks = taskStore.loadPending();
        for (CompensationTaskStore.CompensationTask task : tasks) {
            schedule(task);
        }
    }

    @Override
    public void scheduleCacheWrite(final String cacheKey, final byte[] value, final Duration ttl) {
        CompensationTaskStore.CompensationTask task = taskStore.save(
                CompensationTaskStore.CompensationTask.write(cacheKey, value, ttl));
        observer.onCompensationScheduled(cacheKey, "write");
        schedule(task);
    }

    @Override
    public void scheduleCacheDelete(final String cacheKey) {
        CompensationTaskStore.CompensationTask task = taskStore.save(
                CompensationTaskStore.CompensationTask.delete(cacheKey));
        observer.onCompensationScheduled(cacheKey, "delete");
        schedule(task);
    }

    private void schedule(final CompensationTaskStore.CompensationTask task) {
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                try {
                    execute(task);
                    taskStore.markSuccess(task.getId());
                    observer.onCompensationSuccess(task.getCacheKey(), task.getAction().name().toLowerCase(),
                            task.getAttempt() + 1);
                } catch (RuntimeException error) {
                    int nextAttempt = task.getAttempt() + 1;
                    boolean terminal = nextAttempt > maxRetries;
                    taskStore.markFailure(task.getId(), nextAttempt, error.getMessage(), terminal);
                    observer.onCompensationFailure(task.getCacheKey(),
                            task.getAction().name().toLowerCase(), nextAttempt, error);
                    if (terminal) {
                        log.error("Compensation {} failed for key {} after {} attempts",
                                task.getAction().name().toLowerCase(), task.getCacheKey(), nextAttempt, error);
                        return;
                    }
                    log.warn("Compensation {} retry {} scheduled for key {}",
                            task.getAction().name().toLowerCase(), nextAttempt, task.getCacheKey(), error);
                    schedule(task.withAttempt(nextAttempt));
                }
            }
        }, retryDelayMillis, TimeUnit.MILLISECONDS);
    }

    private void execute(CompensationTaskStore.CompensationTask task) {
        if (task.getAction() == CompensationTaskStore.CompensationTask.Action.WRITE) {
            redisAccessor.set(task.getCacheKey(), task.getPayload(), Duration.ofSeconds(task.getTtlSeconds()));
            return;
        }
        redisAccessor.delete(task.getCacheKey());
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
