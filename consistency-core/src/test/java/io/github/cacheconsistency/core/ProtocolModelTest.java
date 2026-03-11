package io.github.cacheconsistency.core;

import io.github.cacheconsistency.core.protocol.ProtocolConstants;
import io.github.cacheconsistency.core.protocol.ProtocolKeys;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ProtocolModelTest {
    @Test
    void shouldBuildProtocolKeysWithPublicSegments() {
        ProtocolKeys keys = new ProtocolKeys("demo");

        assertEquals("demo:" + ProtocolConstants.DATA_SEGMENT + ":user:1", keys.data("user:1"));
        assertEquals("demo:" + ProtocolConstants.STATUS_SEGMENT + ":user:1", keys.status("user:1"));
        assertEquals("demo:" + ProtocolConstants.VALIDITY_SEGMENT + ":user:1", keys.validity("user:1"));
        assertEquals("demo:" + ProtocolConstants.LEASE_SEGMENT + ":user:1", keys.lease("user:1"));
        assertEquals("demo:" + ProtocolConstants.PROTOCOL_LEASE_SEGMENT + ":user:1", keys.protocolLease("user:1"));
    }

    @Test
    void shouldParseProtocolStateAndValidity() {
        assertEquals(ProtocolState.IN_WRITING, ProtocolState.fromValue("IN_WRITING"));
        assertEquals(ProtocolState.LAST_WRITE_SUCCESS, ProtocolState.fromValue("LAST_WRITE_SUCCESS"));
        assertNull(ProtocolState.fromValue("UNKNOWN"));

        assertEquals(ProtocolValidity.VALID, ProtocolValidity.fromValue("1"));
        assertEquals(ProtocolValidity.INVALID, ProtocolValidity.fromValue("0"));
        assertNull(ProtocolValidity.fromValue("2"));
    }
}
