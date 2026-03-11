package io.github.cacheconsistency.compensation;

import io.github.cacheconsistency.core.ConsistencyContext;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CompensationFinalizeFailureHandlerTest {
    @Test
    void shouldScheduleCacheWriteOnSetFinalizeFailure() {
        RecordingCompensationExecutor compensationExecutor = new RecordingCompensationExecutor();
        CompensationFinalizeFailureHandler handler = new CompensationFinalizeFailureHandler(compensationExecutor);
        byte[] payload = "value".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        handler.onSetFinalizeFailure(
                "order:1",
                "test:data:order:1",
                payload,
                Duration.ofMinutes(1),
                ConsistencyContext.create(),
                new RuntimeException("boom")
        );

        assertEquals("write", compensationExecutor.action);
        assertEquals("test:data:order:1", compensationExecutor.cacheKey);
        assertArrayEquals(payload, compensationExecutor.payload);
        assertEquals(Duration.ofMinutes(1), compensationExecutor.ttl);
    }

    @Test
    void shouldScheduleCacheDeleteOnDeleteFinalizeFailure() {
        RecordingCompensationExecutor compensationExecutor = new RecordingCompensationExecutor();
        CompensationFinalizeFailureHandler handler = new CompensationFinalizeFailureHandler(compensationExecutor);

        handler.onDeleteFinalizeFailure(
                "order:2",
                "test:data:order:2",
                ConsistencyContext.create(),
                new RuntimeException("boom")
        );

        assertEquals("delete", compensationExecutor.action);
        assertEquals("test:data:order:2", compensationExecutor.cacheKey);
    }

    private static final class RecordingCompensationExecutor implements CompensationExecutor {
        private String action;
        private String cacheKey;
        private byte[] payload;
        private Duration ttl;

        @Override
        public void scheduleCacheWrite(String cacheKey, byte[] value, Duration ttl) {
            this.action = "write";
            this.cacheKey = cacheKey;
            this.payload = value;
            this.ttl = ttl;
        }

        @Override
        public void scheduleCacheDelete(String cacheKey) {
            this.action = "delete";
            this.cacheKey = cacheKey;
        }
    }
}
