package io.github.cacheconsistency.spring;

import io.github.cacheconsistency.compensation.CompensationExecutor;
import io.github.cacheconsistency.compensation.CompensationTaskStore;
import io.github.cacheconsistency.core.ConsistencyClient;
import io.github.cacheconsistency.core.ConsistencySettings;
import io.github.cacheconsistency.core.PersistentOperation;
import io.github.cacheconsistency.core.Serializer;
import io.github.cacheconsistency.core.StringSerializer;
import io.github.cacheconsistency.core.VersionHealingOperation;
import io.github.cacheconsistency.core.support.FinalizeFailureHandler;
import io.github.cacheconsistency.core.support.ProtocolInspector;
import io.github.cacheconsistency.core.support.RedisAccessor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CacheConsistencyAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CacheConsistencyAutoConfiguration.class))
            .withUserConfiguration(TestConfiguration.class)
            .withPropertyValues(
                    "cck.key-prefix=boot-demo",
                    "cck.ghost-write-healing-enabled=true",
                    "cck.metrics.enabled=true"
            );

    @Test
    void shouldAutoConfigureCoreBeans() {
        contextRunner.run(context -> {
            assertNotNull(context.getBean(ConsistencyClient.class));
            assertNotNull(context.getBean(ProtocolInspector.class));
            assertNotNull(context.getBean(CacheConsistencyProperties.class));
            assertNotNull(context.getBean(SimpleMeterRegistry.class));
        });
    }

    @Test
    void shouldAutoConfigureCompensationBeansWhenEnabled() throws Exception {
        Path storePath = Files.createTempFile("cck-compensation-autoconfig", ".log");
        try {
            contextRunner.withPropertyValues(
                    "cck.compensation.enabled=true",
                    "cck.compensation.store-path=" + storePath.toString(),
                    "cck.compensation.max-retries=4",
                    "cck.compensation.retry-delay-millis=25",
                    "cck.compensation.replay-pending-on-startup=false",
                    "cck.batch-parallelism=3"
            ).run(context -> {
                assertNotNull(context.getBean(CompensationExecutor.class));
                assertNotNull(context.getBean(FinalizeFailureHandler.class));
                assertNotNull(context.getBean(CompensationTaskStore.class));
                assertNotNull(context.getBean("compensationPendingGauge"));
                ConsistencySettings settings = context.getBean(ConsistencySettings.class);
                assertEquals(3, settings.getBatchParallelism());
            });
        } finally {
            Files.deleteIfExists(storePath);
        }
    }

    @org.springframework.context.annotation.Configuration
    static class TestConfiguration {
        @org.springframework.context.annotation.Bean
        RedisAccessor redisAccessor() {
            RedisAccessor redisAccessor = mock(RedisAccessor.class);
            when(redisAccessor.setIfAbsent(org.mockito.ArgumentMatchers.anyString(),
                    org.mockito.ArgumentMatchers.any(byte[].class),
                    org.mockito.ArgumentMatchers.any(Duration.class))).thenReturn(true);
            return redisAccessor;
        }

        @org.springframework.context.annotation.Bean
        PersistentOperation<String> persistentOperation() {
            @SuppressWarnings("unchecked")
            PersistentOperation<String> operation = mock(PersistentOperation.class);
            return operation;
        }

        @org.springframework.context.annotation.Bean
        Serializer<String> serializer() {
            return StringSerializer.UTF8;
        }

        @org.springframework.context.annotation.Bean
        VersionHealingOperation versionHealingOperation() {
            return new VersionHealingOperation() {
                @Override
                public void healVersion(String key, io.github.cacheconsistency.core.ConsistencyContext context) {
                }
            };
        }

        @org.springframework.context.annotation.Bean
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
