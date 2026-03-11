package io.github.cacheconsistency.spring;

import io.github.cacheconsistency.core.ConsistencyClient;
import io.github.cacheconsistency.core.ConsistencySettings;
import io.github.cacheconsistency.core.PersistentOperation;
import io.github.cacheconsistency.core.Serializer;
import io.github.cacheconsistency.core.support.ConsistencyObserver;
import io.github.cacheconsistency.core.support.DefaultConsistencyClient;
import io.github.cacheconsistency.core.support.FinalizeFailureHandler;
import io.github.cacheconsistency.core.support.GhostWriteHealer;
import io.github.cacheconsistency.core.support.RedisAccessor;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Classic Spring factory bean for environments that do not use Spring Boot auto-configuration.
 */
public class ConsistencyClientFactoryBean<T> implements FactoryBean<ConsistencyClient<T>>, InitializingBean, DisposableBean {
    private RedisAccessor redisAccessor;
    private PersistentOperation<T> persistentOperation;
    private Serializer<T> serializer;
    private ConsistencySettings settings = ConsistencySettings.builder().build();
    private ConsistencyObserver observer = ConsistencyObserver.NoOpConsistencyObserver.INSTANCE;
    private GhostWriteHealer ghostWriteHealer = GhostWriteHealer.NoOpGhostWriteHealer.INSTANCE;
    private FinalizeFailureHandler finalizeFailureHandler = FinalizeFailureHandler.NoOpFinalizeFailureHandler.INSTANCE;
    private ConsistencyClient<T> object;

    public void setRedisAccessor(RedisAccessor redisAccessor) {
        this.redisAccessor = redisAccessor;
    }

    public void setPersistentOperation(PersistentOperation<T> persistentOperation) {
        this.persistentOperation = persistentOperation;
    }

    public void setSerializer(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    public void setSettings(ConsistencySettings settings) {
        this.settings = settings;
    }

    public void setObserver(ConsistencyObserver observer) {
        this.observer = observer;
    }

    public void setGhostWriteHealer(GhostWriteHealer ghostWriteHealer) {
        this.ghostWriteHealer = ghostWriteHealer;
    }

    public void setFinalizeFailureHandler(FinalizeFailureHandler finalizeFailureHandler) {
        this.finalizeFailureHandler = finalizeFailureHandler;
    }

    @Override
    public void afterPropertiesSet() {
        if (redisAccessor == null || persistentOperation == null || serializer == null) {
            throw new IllegalArgumentException("redisAccessor, persistentOperation and serializer are required");
        }
        this.object = new DefaultConsistencyClient<T>(
                redisAccessor,
                persistentOperation,
                serializer,
                settings,
                observer,
                ghostWriteHealer,
                finalizeFailureHandler
        );
    }

    @Override
    public ConsistencyClient<T> getObject() {
        return object;
    }

    @Override
    public Class<?> getObjectType() {
        return ConsistencyClient.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void destroy() {
        if (object != null) {
            object.shutdown();
        }
    }
}
