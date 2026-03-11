package io.github.cacheconsistency.testkit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ConcurrentTestHarness {
    private ConcurrentTestHarness() {
    }

    public static <T> List<T> runConcurrently(int concurrency, final Callable<T> task) throws Exception {
        ExecutorService executor = new ThreadPoolExecutor(
                concurrency,
                concurrency,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<Future<T>>(concurrency);
            for (int index = 0; index < concurrency; index++) {
                futures.add(executor.submit(new Callable<T>() {
                    @Override
                    public T call() throws Exception {
                        ready.countDown();
                        start.await();
                        return task.call();
                    }
                }));
            }
            ready.await();
            start.countDown();
            List<T> results = new ArrayList<T>(concurrency);
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        } finally {
            executor.shutdownNow();
        }
    }
}
