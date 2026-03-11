package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolSnapshot;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only view into the Redis protocol state for diagnostics and tooling.
 */
public interface ProtocolInspector {
    ProtocolSnapshot snapshot(String key);

    BatchReadDebugSnapshot explainRead(String key);

    default Map<String, ProtocolSnapshot> snapshotAll(Collection<String> keys) {
        Map<String, ProtocolSnapshot> result = new LinkedHashMap<String, ProtocolSnapshot>();
        for (String key : keys) {
            result.put(key, snapshot(key));
        }
        return result;
    }

    default Map<String, BatchReadDebugSnapshot> explainReadAll(Collection<String> keys) {
        Map<String, BatchReadDebugSnapshot> result = new LinkedHashMap<String, BatchReadDebugSnapshot>();
        for (String key : keys) {
            result.put(key, explainRead(key));
        }
        return result;
    }
}
