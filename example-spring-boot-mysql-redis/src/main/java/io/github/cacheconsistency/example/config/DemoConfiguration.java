package io.github.cacheconsistency.example.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cacheconsistency.compensation.AsyncCompensationExecutor;
import io.github.cacheconsistency.compensation.CompensationExecutor;
import io.github.cacheconsistency.compensation.CompensationFinalizeFailureHandler;
import io.github.cacheconsistency.compensation.FileCompensationTaskStore;
import io.github.cacheconsistency.core.Serializer;
import io.github.cacheconsistency.core.VersionHealingOperation;
import io.github.cacheconsistency.core.support.ConsistencyObserver;
import io.github.cacheconsistency.core.support.FinalizeFailureHandler;
import io.github.cacheconsistency.core.support.RedisAccessor;
import io.github.cacheconsistency.example.model.UserProfile;
import io.github.cacheconsistency.example.support.JacksonSerializer;
import io.github.cacheconsistency.redis.LettuceRedisAccessor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Paths;

/**
 * Demo-only infrastructure configuration.
 */
@Configuration
@EnableConfigurationProperties({DemoRedisProperties.class, DemoCompensationProperties.class})
public class DemoConfiguration {
    @Bean(destroyMethod = "shutdown")
    public RedisClient redisClient(DemoRedisProperties properties) {
        return RedisClient.create(properties.getRedisUrl());
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisConnection<byte[], byte[]> redisConnection(RedisClient redisClient) {
        return LettuceRedisAccessor.connect(redisClient);
    }

    @Bean
    public RedisAccessor redisAccessor(StatefulRedisConnection<byte[], byte[]> redisConnection) {
        return new LettuceRedisAccessor(redisConnection);
    }

    @Bean
    public Serializer<UserProfile> userProfileSerializer(ObjectMapper objectMapper) {
        return new JacksonSerializer<UserProfile>(objectMapper, UserProfile.class);
    }

    @Bean
    public VersionHealingOperation versionHealingOperation(io.github.cacheconsistency.example.store.UserProfileStore store) {
        return new VersionHealingOperation() {
            @Override
            public void healVersion(String key, io.github.cacheconsistency.core.ConsistencyContext context) {
                // The demo healer follows the protocol rule: advance only the version, never mutate business data.
                store.bumpVersion(key);
            }
        };
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty(prefix = "demo.compensation", name = "enabled", havingValue = "true")
    public AsyncCompensationExecutor compensationExecutor(RedisAccessor redisAccessor,
                                                          DemoCompensationProperties properties,
                                                          ConsistencyObserver observer) {
        AsyncCompensationExecutor executor = new AsyncCompensationExecutor(
                redisAccessor,
                properties.getMaxRetries(),
                properties.getRetryDelayMillis(),
                new FileCompensationTaskStore(Paths.get(properties.getStorePath())),
                observer
        );
        executor.replayPending();
        return executor;
    }

    @Bean
    @ConditionalOnBean(CompensationExecutor.class)
    public FinalizeFailureHandler finalizeFailureHandler(CompensationExecutor compensationExecutor) {
        return new CompensationFinalizeFailureHandler(compensationExecutor);
    }
}
