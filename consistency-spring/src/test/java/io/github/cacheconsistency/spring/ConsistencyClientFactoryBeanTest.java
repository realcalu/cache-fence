package io.github.cacheconsistency.spring;

import io.github.cacheconsistency.core.ConsistencyClient;
import io.github.cacheconsistency.core.PersistentOperation;
import io.github.cacheconsistency.core.StringSerializer;
import io.github.cacheconsistency.core.support.RedisAccessor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class ConsistencyClientFactoryBeanTest {
    @Test
    void shouldCreateClient() throws Exception {
        ConsistencyClientFactoryBean<String> factoryBean = new ConsistencyClientFactoryBean<String>();
        factoryBean.setRedisAccessor(mock(RedisAccessor.class));
        @SuppressWarnings("unchecked")
        PersistentOperation<String> persistentOperation = mock(PersistentOperation.class);
        factoryBean.setPersistentOperation(persistentOperation);
        factoryBean.setSerializer(StringSerializer.UTF8);
        factoryBean.afterPropertiesSet();

        ConsistencyClient<String> client = factoryBean.getObject();
        assertNotNull(client);
    }
}
