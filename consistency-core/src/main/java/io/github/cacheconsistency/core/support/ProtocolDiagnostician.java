package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.protocol.ProtocolDiagnosis;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Produces human-readable diagnoses from raw protocol state.
 */
public interface ProtocolDiagnostician {
    ProtocolDiagnosis diagnose(String key);

    default Map<String, ProtocolDiagnosis> diagnoseAll(Collection<String> keys) {
        Map<String, ProtocolDiagnosis> result = new LinkedHashMap<String, ProtocolDiagnosis>();
        for (String key : keys) {
            result.put(key, diagnose(key));
        }
        return result;
    }
}
