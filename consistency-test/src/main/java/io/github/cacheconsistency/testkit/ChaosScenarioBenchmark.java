package io.github.cacheconsistency.testkit;

import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.ConsistencySettings;
import io.github.cacheconsistency.core.ReadResult;
import io.github.cacheconsistency.core.StringSerializer;
import io.github.cacheconsistency.core.WriteResult;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import io.github.cacheconsistency.core.support.DefaultConsistencyClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ChaosScenarioBenchmark {
    private static final Duration TTL = Duration.ofMinutes(5);

    private ChaosScenarioBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        int iterations = parseIterations(args, 100);
        List<ScenarioResult> results = new ArrayList<ScenarioResult>();
        results.add(runScenario("场景1 客户端超时", "客户端超时", iterations, new ScenarioCheck() {
            @Override
            public boolean run() throws Exception {
                return scenario1ClientTimeout();
            }
        }));
        results.add(runScenario("场景2 阻塞点未恢复", "任意步骤阻塞", iterations, new ScenarioCheck() {
            @Override
            public boolean run() throws Exception {
                return scenario2BlockedStep();
            }
        }));
        results.add(runScenario("场景3 数据库操作失败", "KeyVersion查询失败/写超时", iterations, new ScenarioCheck() {
            @Override
            public boolean run() throws Exception {
                return scenario3DatabaseFailure();
            }
        }));
        results.add(runScenario("场景4 缩短租约", "Lease=1s", iterations, new ScenarioCheck() {
            @Override
            public boolean run() throws Exception {
                return scenario4ShortLease();
            }
        }));
        results.add(runScenario("场景5 Redis实例Failover", "Redis读失败后恢复", iterations, new ScenarioCheck() {
            @Override
            public boolean run() throws Exception {
                return scenario5RedisFailover();
            }
        }));
        results.add(runScenario("场景6 缓存元数据随机淘汰", "KeyStatus/KeyLeases丢失", iterations, new ScenarioCheck() {
            @Override
            public boolean run() throws Exception {
                return scenario6MetadataLoss();
            }
        }));
        printResults(results);
    }

    private static ScenarioResult runScenario(String name,
                                              String condition,
                                              int iterations,
                                              ScenarioCheck scenarioCheck) throws Exception {
        int consistent = 0;
        for (int index = 0; index < iterations; index++) {
            if (scenarioCheck.run()) {
                consistent++;
            }
        }
        return new ScenarioResult(name, condition, consistent, iterations);
    }

    private static boolean scenario1ClientTimeout() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("scenario:1", "before", 1L);
        store.setUpdateDelayMillis(120L);
        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("bench").build());

        ExecutorService executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        try {
            Future<WriteResult> future = executor.submit(new Callable<WriteResult>() {
                @Override
                public WriteResult call() {
                    return client.set("scenario:1", "after", TTL, ConsistencyContext.create().withVersion("1"));
                }
            });
            boolean timedOut = false;
            try {
                future.get(20L, TimeUnit.MILLISECONDS);
            } catch (TimeoutException expected) {
                timedOut = true;
            }
            WriteResult result = future.get(1L, TimeUnit.SECONDS);
            ReadResult<String> readResult = client.get("scenario:1", TTL, ConsistencyContext.create());
            return timedOut
                    && result.getStatus() == WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED
                    && "after".equals(readResult.getData());
        } finally {
            executor.shutdownNow();
        }
    }

    private static boolean scenario2BlockedStep() throws Exception {
        final InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        final InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("scenario:2", "value", 1L);
        store.setBeforeUpdateHook(new Runnable() {
            @Override
            public void run() {
                sleepSilently(80L);
            }
        });
        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("bench").build());
        ExecutorService executor = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        try {
            Future<WriteResult> future = executor.submit(new Callable<WriteResult>() {
                @Override
                public WriteResult call() {
                    return client.set("scenario:2", "new-value", TTL, ConsistencyContext.create().withVersion("1"));
                }
            });
            Thread.sleep(10L);
            ReadResult<String> duringBlock = client.get("scenario:2", TTL, ConsistencyContext.create());
            WriteResult finalResult = future.get(1L, TimeUnit.SECONDS);
            return duringBlock.getSource() == ReadResult.ReadSource.STORE
                    && "value".equals(duringBlock.getData())
                    && finalResult.getStatus() == WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED;
        } finally {
            executor.shutdownNow();
        }
    }

    private static boolean scenario3DatabaseFailure() {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        store.seed("scenario:3", "stable", 5L);
        store.setQueryVersionFailure(new RuntimeException("version query timeout"));
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("bench").build());
        try {
            client.set("scenario:3", "broken", TTL, ConsistencyContext.create().withVersion("5"));
            return false;
        } catch (RuntimeException ignored) {
            ReadResult<String> readResult = client.get("scenario:3", TTL, ConsistencyContext.create());
            return "stable".equals(readResult.getData()) && readResult.getSource() == ReadResult.ReadSource.STORE;
        }
    }

    private static boolean scenario4ShortLease() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("bench");
        store.seed("scenario:4", "lease-value", 1L);
        store.setReadDelayMillis(1050L);
        redis.set(keys.status("scenario:4"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()),
                TTL);
        final DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("bench").leaseTtlSeconds(1).retryBackoffMillis(10).build()
        );
        List<ReadResult<String>> results = ConcurrentTestHarness.runConcurrently(4, new Callable<ReadResult<String>>() {
            @Override
            public ReadResult<String> call() {
                return client.get("scenario:4", TTL, ConsistencyContext.create());
            }
        });
        for (ReadResult<String> result : results) {
            if (!"lease-value".equals(result.getData())) {
                return false;
            }
        }
        return true;
    }

    private static boolean scenario5RedisFailover() {
        InMemoryRedisAccessor baseRedis = new InMemoryRedisAccessor();
        FaultInjectingRedisAccessor redis = new FaultInjectingRedisAccessor(baseRedis);
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("bench");
        store.seed("scenario:5", "db-value", 1L);
        baseRedis.set(keys.status("scenario:5"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), TTL);
        redis.failNextGets(1);
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis, store, StringSerializer.UTF8, ConsistencySettings.builder().keyPrefix("bench").build());
        ReadResult<String> first = client.get("scenario:5", TTL, ConsistencyContext.create());
        ReadResult<String> second = client.get("scenario:5", TTL, ConsistencyContext.create());
        return "db-value".equals(first.getData()) && "db-value".equals(second.getData());
    }

    private static boolean scenario6MetadataLoss() throws Exception {
        InMemoryRedisAccessor redis = new InMemoryRedisAccessor();
        InMemoryPersistentOperation<String> store = new InMemoryPersistentOperation<String>();
        ProtocolKeys keys = new ProtocolKeys("bench");
        store.seed("scenario:6", "db-value", 9L);
        InMemoryGhostWriteHealer healer = new InMemoryGhostWriteHealer(store);
        DefaultConsistencyClient<String> client = new DefaultConsistencyClient<String>(
                redis,
                store,
                StringSerializer.UTF8,
                ConsistencySettings.builder().keyPrefix("bench").ghostWriteHealingEnabled(true).build(),
                new InMemoryConsistencyObserver(),
                healer
        );
        redis.set(keys.status("scenario:6"), StringSerializer.UTF8.serialize(ProtocolState.LAST_WRITE_SUCCESS.value()), TTL);
        redis.set(keys.validity("scenario:6"), StringSerializer.UTF8.serialize(ProtocolValidity.VALID.value()), TTL);
        redis.delete(keys.status("scenario:6"), keys.protocolLease("scenario:6"), keys.data("scenario:6"));
        ReadResult<String> readResult = client.get("scenario:6", TTL, ConsistencyContext.create());
        Thread.sleep(60L);
        return readResult.getSource() == ReadResult.ReadSource.STORE
                && "db-value".equals(readResult.getData())
                && "10".equals(store.getVersion("scenario:6"));
    }

    private static void printResults(List<ScenarioResult> results) {
        System.out.println("| 场景 | 模拟的混沌条件 | counterConsistent | 结论 |");
        System.out.println("| --- | --- | --- | --- |");
        for (ScenarioResult result : results) {
            System.out.println("| " + result.name + " | "
                    + result.condition + " | "
                    + result.consistent + " / " + result.total + " (" + result.rate() + "%) | "
                    + result.conclusion() + " |");
        }
    }

    private static int parseIterations(String[] args, int defaultValue) {
        for (String arg : args) {
            if (arg.startsWith("--iterations=")) {
                return Integer.parseInt(arg.substring("--iterations=".length()));
            }
        }
        return defaultValue;
    }

    private static void sleepSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private interface ScenarioCheck {
        boolean run() throws Exception;
    }

    private static final class ScenarioResult {
        private final String name;
        private final String condition;
        private final int consistent;
        private final int total;

        private ScenarioResult(String name, String condition, int consistent, int total) {
            this.name = name;
            this.condition = condition;
            this.consistent = consistent;
            this.total = total;
        }

        private String rate() {
            if (total == 0) {
                return "0.00";
            }
            double rate = (consistent * 100.0d) / total;
            return String.format(java.util.Locale.ROOT, "%.2f", rate);
        }

        private String conclusion() {
            return consistent == total ? "读一致性保持" : "存在失败样本，需要继续排查";
        }
    }
}
