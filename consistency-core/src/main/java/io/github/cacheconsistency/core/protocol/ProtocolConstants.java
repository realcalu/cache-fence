package io.github.cacheconsistency.core.protocol;

/**
 * Public protocol constants for key segments and canonical string values.
 */
public final class ProtocolConstants {
    public static final String VALID = ProtocolValidity.VALID.value();
    public static final String INVALID = ProtocolValidity.INVALID.value();
    public static final String DATA_SEGMENT = "data";
    public static final String STATUS_SEGMENT = "status";
    public static final String VALIDITY_SEGMENT = "validity";
    public static final String LEASE_SEGMENT = "lease";
    public static final String PROTOCOL_LEASE_SEGMENT = "protocol-lease";

    private ProtocolConstants() {
    }
}
