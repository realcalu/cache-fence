package io.github.cacheconsistency.core;

import io.github.cacheconsistency.core.protocol.ProtocolDiagnosis;
import io.github.cacheconsistency.core.protocol.ProtocolSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import io.github.cacheconsistency.core.support.DefaultProtocolDiagnostician;
import io.github.cacheconsistency.core.support.ProtocolInspector;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultProtocolDiagnosticianTest {
    @Test
    void shouldDiagnoseGhostWriteSuspicion() {
        ProtocolInspector inspector = mock(ProtocolInspector.class);
        when(inspector.snapshot("ghost:1")).thenReturn(
                new ProtocolSnapshot("ghost:1", false, null, null, null, null)
        );

        ProtocolDiagnosis diagnosis = new DefaultProtocolDiagnostician(inspector).diagnose("ghost:1");

        assertTrue(diagnosis.getSummary().contains("ghost write"));
        assertTrue(diagnosis.format().contains("recommendedAction="));
    }

    @Test
    void shouldDiagnoseHealthyReadableCache() {
        ProtocolInspector inspector = mock(ProtocolInspector.class);
        when(inspector.snapshot("user:1")).thenReturn(
                new ProtocolSnapshot("user:1", true, ProtocolState.LAST_WRITE_SUCCESS,
                        ProtocolValidity.VALID, "r1", null)
        );

        ProtocolDiagnosis diagnosis = new DefaultProtocolDiagnostician(inspector).diagnose("user:1");

        assertTrue(diagnosis.getSummary().contains("Cache is readable"));
        assertTrue(diagnosis.getRecommendedAction().contains("Serve from cache"));
    }

    @Test
    void shouldDiagnoseInBatch() {
        ProtocolInspector inspector = mock(ProtocolInspector.class);
        when(inspector.snapshot("user:1")).thenReturn(
                new ProtocolSnapshot("user:1", true, ProtocolState.LAST_WRITE_SUCCESS,
                        ProtocolValidity.VALID, "r1", null)
        );
        when(inspector.snapshot("ghost:1")).thenReturn(
                new ProtocolSnapshot("ghost:1", false, null, null, null, null)
        );

        Map<String, ProtocolDiagnosis> diagnoses = new DefaultProtocolDiagnostician(inspector)
                .diagnoseAll(Arrays.asList("user:1", "ghost:1"));

        assertEquals(2, diagnoses.size());
        assertTrue(diagnoses.get("user:1").getSummary().contains("Cache is readable"));
        assertTrue(diagnoses.get("ghost:1").getSummary().contains("ghost write"));
    }
}
