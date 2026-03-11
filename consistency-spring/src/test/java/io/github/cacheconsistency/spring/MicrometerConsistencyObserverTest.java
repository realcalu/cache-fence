package io.github.cacheconsistency.spring;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MicrometerConsistencyObserverTest {
    @Test
    void shouldPublishCounters() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MicrometerConsistencyObserver observer = new MicrometerConsistencyObserver(meterRegistry);

        observer.onCacheHit("a");
        observer.onCacheMiss("a");
        observer.onStoreWrite("a");
        observer.onVersionRejected("a");
        observer.onFinalizeFailure("a", "set", new RuntimeException("boom"));
        observer.onGhostWriteHealScheduled("a");
        observer.onGhostWriteHealSuccess("a");
        observer.onGhostWriteHealFailure("a", new RuntimeException("boom"));
        observer.onBatchOperation("get", 3, true);
        observer.onBatchOperation("set", 2, false);
        observer.onCompensationScheduled("a", "set");
        observer.onCompensationSuccess("a", "set", 1);
        observer.onCompensationFailure("a", "set", 2, new RuntimeException("boom"));

        assertEquals(1.0d, meterRegistry.get("cck.cache.hit").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.cache.miss").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.store.write").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.version.rejected").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.finalize.failure").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.ghost_heal.scheduled").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.ghost_heal.success").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.ghost_heal.failure").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.batch.invocation").tags("action", "get", "optimized", "true").counter().count());
        assertEquals(3.0d, meterRegistry.get("cck.batch.items").tags("action", "get", "optimized", "true").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.batch.invocation").tags("action", "set", "optimized", "false").counter().count());
        assertEquals(2.0d, meterRegistry.get("cck.batch.items").tags("action", "set", "optimized", "false").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.compensation.scheduled").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.compensation.success").counter().count());
        assertEquals(1.0d, meterRegistry.get("cck.compensation.failure").counter().count());
    }
}
