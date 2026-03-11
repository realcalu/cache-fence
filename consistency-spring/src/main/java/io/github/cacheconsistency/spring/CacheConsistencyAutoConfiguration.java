package io.github.cacheconsistency.spring;

import io.github.cacheconsistency.compensation.AsyncCompensationExecutor;
import io.github.cacheconsistency.compensation.CompensationExecutor;
import io.github.cacheconsistency.compensation.CompensationFinalizeFailureHandler;
import io.github.cacheconsistency.compensation.CompensationTaskStore;
import io.github.cacheconsistency.compensation.FileCompensationTaskStore;
import io.github.cacheconsistency.core.ConsistencyClient;
import io.github.cacheconsistency.core.ConsistencySettings;
import io.github.cacheconsistency.core.PersistentOperation;
import io.github.cacheconsistency.core.Serializer;
import io.github.cacheconsistency.core.VersionHealingOperation;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.support.ConsistencyObserver;
import io.github.cacheconsistency.core.support.DefaultProtocolDiagnostician;
import io.github.cacheconsistency.core.support.DefaultConsistencyClient;
import io.github.cacheconsistency.core.support.FinalizeFailureHandler;
import io.github.cacheconsistency.core.support.GhostWriteHealer;
import io.github.cacheconsistency.core.support.ProtocolDiagnostician;
import io.github.cacheconsistency.core.support.ProtocolInspector;
import io.github.cacheconsistency.core.support.RedisAccessor;
import io.github.cacheconsistency.core.support.RedisProtocolInspector;
import io.github.cacheconsistency.core.support.AsyncGhostWriteHealer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Spring Boot auto-configuration for the consistency client and its optional observability helpers.
 */
@Configuration
@ConditionalOnClass(ConsistencyClient.class)
@EnableConfigurationProperties(CacheConsistencyProperties.class)
public class CacheConsistencyAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public ConsistencySettings consistencySettings(CacheConsistencyProperties properties) {
        // Keep Boot properties as a thin facade over the immutable core settings object.
        return ConsistencySettings.builder()
                .leaseTtlSeconds(properties.getLeaseTtlSeconds())
                .writeLockTtlSeconds(properties.getWriteLockTtlSeconds())
                .retryBackoffMillis(properties.getRetryBackoffMillis())
                .ghostWriteHealingEnabled(properties.isGhostWriteHealingEnabled())
                .batchParallelism(properties.getBatchParallelism())
                .keyPrefix(properties.getKeyPrefix())
                .build();
    }

    @Bean
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnMissingBean(name = "micrometerConsistencyObserver")
    @ConditionalOnProperty(prefix = "cck.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
    public ConsistencyObserver micrometerConsistencyObserver(MeterRegistry meterRegistry) {
        return new MicrometerConsistencyObserver(meterRegistry);
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean(GhostWriteHealer.class)
    public GhostWriteHealer ghostWriteHealer(ObjectProvider<VersionHealingOperation> healingOperationProvider,
                                             ObjectProvider<ConsistencyObserver> observers) {
        VersionHealingOperation healingOperation = healingOperationProvider.getIfAvailable();
        if (healingOperation == null) {
            return GhostWriteHealer.NoOpGhostWriteHealer.INSTANCE;
        }
        return new AsyncGhostWriteHealer(healingOperation, compositeObserver(observers));
    }

    @Bean
    @ConditionalOnMissingBean(ProtocolInspector.class)
    public ProtocolInspector protocolInspector(RedisAccessor redisAccessor, ConsistencySettings settings) {
        return new RedisProtocolInspector(redisAccessor, new ProtocolKeys(settings.getKeyPrefix()));
    }

    @Bean
    @ConditionalOnMissingBean(ProtocolDiagnostician.class)
    public ProtocolDiagnostician protocolDiagnostician(ProtocolInspector protocolInspector) {
        return new DefaultProtocolDiagnostician(protocolInspector);
    }

    @Bean
    @ConditionalOnClass(AsyncCompensationExecutor.class)
    @ConditionalOnMissingBean(CompensationTaskStore.class)
    @ConditionalOnProperty(prefix = "cck.compensation", name = "enabled", havingValue = "true")
    public CompensationTaskStore compensationTaskStore(CacheConsistencyProperties properties) {
        Path path = Paths.get(properties.getCompensation().getStorePath());
        Path parent = path.getParent();
        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException exception) {
                throw new IllegalStateException("Failed to create compensation store directory", exception);
            }
        }
        return new FileCompensationTaskStore(path);
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnClass(AsyncCompensationExecutor.class)
    @ConditionalOnMissingBean(CompensationExecutor.class)
    @ConditionalOnProperty(prefix = "cck.compensation", name = "enabled", havingValue = "true")
    public CompensationExecutor compensationExecutor(RedisAccessor redisAccessor,
                                                     CacheConsistencyProperties properties,
                                                     CompensationTaskStore taskStore,
                                                     ObjectProvider<ConsistencyObserver> observers) {
        AsyncCompensationExecutor executor = new AsyncCompensationExecutor(
                redisAccessor,
                properties.getCompensation().getMaxRetries(),
                properties.getCompensation().getRetryDelayMillis(),
                taskStore,
                compositeObserver(observers)
        );
        if (properties.getCompensation().isReplayPendingOnStartup()) {
            executor.replayPending();
        }
        return executor;
    }

    @Bean
    @ConditionalOnClass(CompensationFinalizeFailureHandler.class)
    @ConditionalOnMissingBean(FinalizeFailureHandler.class)
    @ConditionalOnBean(CompensationExecutor.class)
    public FinalizeFailureHandler finalizeFailureHandler(CompensationExecutor compensationExecutor) {
        return new CompensationFinalizeFailureHandler(compensationExecutor);
    }

    @Bean
    @ConditionalOnBean({MeterRegistry.class, CompensationTaskStore.class})
    @ConditionalOnProperty(prefix = "cck.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
    public Gauge compensationPendingGauge(MeterRegistry meterRegistry, CompensationTaskStore taskStore) {
        return Gauge.builder("cck.compensation.pending", taskStore, new java.util.function.ToDoubleFunction<CompensationTaskStore>() {
            @Override
            public double applyAsDouble(CompensationTaskStore value) {
                return value.loadPending().size();
            }
        }).register(meterRegistry);
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    @ConditionalOnBean({RedisAccessor.class, PersistentOperation.class, Serializer.class})
    public <T> ConsistencyClient<T> consistencyClient(RedisAccessor redisAccessor,
                                                      PersistentOperation<T> persistentOperation,
                                                      Serializer<T> serializer,
                                                      ConsistencySettings settings,
                                                      GhostWriteHealer ghostWriteHealer,
                                                      ObjectProvider<FinalizeFailureHandler> finalizeFailureHandlerProvider,
                                                      ObjectProvider<ConsistencyObserver> observers) {
        ConsistencyObserver consistencyObserver = compositeObserver(observers);
        FinalizeFailureHandler finalizeFailureHandler =
                finalizeFailureHandlerProvider.getIfAvailable(
                        new java.util.function.Supplier<FinalizeFailureHandler>() {
                            @Override
                            public FinalizeFailureHandler get() {
                                return FinalizeFailureHandler.NoOpFinalizeFailureHandler.INSTANCE;
                            }
                        }
                );
        // Auto-config wires the same client used in manual integration so Boot users stay on the core path.
        return new DefaultConsistencyClient<T>(
                redisAccessor,
                persistentOperation,
                serializer,
                settings,
                consistencyObserver,
                ghostWriteHealer,
                finalizeFailureHandler
        );
    }

    private ConsistencyObserver compositeObserver(ObjectProvider<ConsistencyObserver> observers) {
        List<ConsistencyObserver> observerList = new ArrayList<ConsistencyObserver>();
        for (ConsistencyObserver observer : observers) {
            observerList.add(observer);
        }
        if (observerList.isEmpty()) {
            return ConsistencyObserver.NoOpConsistencyObserver.INSTANCE;
        }
        if (observerList.size() == 1) {
            return observerList.get(0);
        }
        return new CompositeConsistencyObserver(observerList);
    }

}
