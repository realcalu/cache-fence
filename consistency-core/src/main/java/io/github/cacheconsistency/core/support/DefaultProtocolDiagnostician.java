package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.protocol.ProtocolDiagnosis;
import io.github.cacheconsistency.core.protocol.ProtocolSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;

/**
 * Default diagnosis rules for interpreting protocol snapshots.
 */
public final class DefaultProtocolDiagnostician implements ProtocolDiagnostician {
    private final ProtocolInspector protocolInspector;

    public DefaultProtocolDiagnostician(ProtocolInspector protocolInspector) {
        this.protocolInspector = protocolInspector;
    }

    @Override
    public ProtocolDiagnosis diagnose(String key) {
        ProtocolSnapshot snapshot = protocolInspector.snapshot(key);
        if (snapshot.isGhostWriteSuspected()) {
            return new ProtocolDiagnosis(
                    snapshot,
                    "Protocol state missing while data key is absent; ghost write is suspected.",
                    "Read from store only and consider enabling ghost write healing."
            );
        }
        if (snapshot.isWriteInFlight()) {
            return new ProtocolDiagnosis(
                    snapshot,
                    "Write is currently in flight; cache should not be trusted.",
                    "Read from store without cache backfill until write finalizes. If this persists past write TTL, inspect finalize failures or compensation backlog."
            );
        }
        if (snapshot.isDataPresent() && snapshot.getValidity() == ProtocolValidity.INVALID) {
            return new ProtocolDiagnosis(
                    snapshot,
                    "Cached value exists but is explicitly marked invalid.",
                    "Bypass cache and read from store."
            );
        }
        if (snapshot.isDataPresent()
                && snapshot.getState() == ProtocolState.LAST_WRITE_SUCCESS
                && snapshot.getValidity() == ProtocolValidity.VALID) {
            return new ProtocolDiagnosis(
                    snapshot,
                    "Cache is readable and protocol state indicates last write completed successfully.",
                    "Serve from cache."
            );
        }
        if (!snapshot.isDataPresent() && snapshot.getState() == ProtocolState.LAST_WRITE_SUCCESS) {
            return new ProtocolDiagnosis(
                    snapshot,
                    "Cache data is absent but protocol state is healthy.",
                    "Acquire read lease, read from store, and backfill cache."
            );
        }
        return new ProtocolDiagnosis(
                snapshot,
                "Protocol state is incomplete or transitional.",
                "Inspect store state, protocol leases, and any finalize-failure or compensation signals before manual intervention."
        );
    }
}
