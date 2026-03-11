package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolState;
import io.github.cacheconsistency.core.protocol.ProtocolValidity;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * Optional batch extension for Redis accessors.
 */
public interface BatchRedisAccessor extends RedisAccessor {
    Map<String, byte[]> getAll(Collection<String> keys);

    void setAll(Map<String, byte[]> values, Duration ttl);

    Map<String, Boolean> beginWriteAll(Collection<BeginWriteRequest> requests);

    Map<String, Boolean> stageCacheValueAll(Collection<StageCacheValueRequest> requests);

    Map<String, Boolean> finalizeWriteAll(Collection<FinalizeWriteRequest> requests);

    Map<String, ReadProtocolResult> readProtocolAll(Collection<ReadProtocolRequest> requests);

    final class BeginWriteRequest {
        private final String key;
        private final String statusKey;
        private final String protocolLeaseKey;
        private final String dataKey;
        private final String validityKey;
        private final byte[] leaseToken;
        private final Duration writeWindowTtl;

        public BeginWriteRequest(String key,
                                 String statusKey,
                                 String protocolLeaseKey,
                                 String dataKey,
                                 String validityKey,
                                 byte[] leaseToken,
                                 Duration writeWindowTtl) {
            this.key = key;
            this.statusKey = statusKey;
            this.protocolLeaseKey = protocolLeaseKey;
            this.dataKey = dataKey;
            this.validityKey = validityKey;
            this.leaseToken = leaseToken;
            this.writeWindowTtl = writeWindowTtl;
        }

        public String getKey() {
            return key;
        }

        public String getStatusKey() {
            return statusKey;
        }

        public String getProtocolLeaseKey() {
            return protocolLeaseKey;
        }

        public String getDataKey() {
            return dataKey;
        }

        public String getValidityKey() {
            return validityKey;
        }

        public byte[] getLeaseToken() {
            return leaseToken;
        }

        public Duration getWriteWindowTtl() {
            return writeWindowTtl;
        }
    }

    final class StageCacheValueRequest {
        private final String key;
        private final String protocolLeaseKey;
        private final byte[] expectedValue;
        private final String cacheKey;
        private final byte[] cacheValue;
        private final Duration ttl;

        public StageCacheValueRequest(String key,
                                      String protocolLeaseKey,
                                      byte[] expectedValue,
                                      String cacheKey,
                                      byte[] cacheValue,
                                      Duration ttl) {
            this.key = key;
            this.protocolLeaseKey = protocolLeaseKey;
            this.expectedValue = expectedValue;
            this.cacheKey = cacheKey;
            this.cacheValue = cacheValue;
            this.ttl = ttl;
        }

        public String getKey() {
            return key;
        }

        public String getProtocolLeaseKey() {
            return protocolLeaseKey;
        }

        public byte[] getExpectedValue() {
            return expectedValue;
        }

        public String getCacheKey() {
            return cacheKey;
        }

        public byte[] getCacheValue() {
            return cacheValue;
        }

        public Duration getTtl() {
            return ttl;
        }
    }

    final class FinalizeWriteRequest {
        private final String key;
        private final String statusKey;
        private final String protocolLeaseKey;
        private final String validityKey;
        private final String dataKey;
        private final byte[] leaseToken;
        private final Duration statusTtl;
        private final Duration dataTtl;

        public FinalizeWriteRequest(String key,
                                    String statusKey,
                                    String protocolLeaseKey,
                                    String validityKey,
                                    String dataKey,
                                    byte[] leaseToken,
                                    Duration statusTtl,
                                    Duration dataTtl) {
            this.key = key;
            this.statusKey = statusKey;
            this.protocolLeaseKey = protocolLeaseKey;
            this.validityKey = validityKey;
            this.dataKey = dataKey;
            this.leaseToken = leaseToken;
            this.statusTtl = statusTtl;
            this.dataTtl = dataTtl;
        }

        public String getKey() {
            return key;
        }

        public String getStatusKey() {
            return statusKey;
        }

        public String getProtocolLeaseKey() {
            return protocolLeaseKey;
        }

        public String getValidityKey() {
            return validityKey;
        }

        public String getDataKey() {
            return dataKey;
        }

        public byte[] getLeaseToken() {
            return leaseToken;
        }

        public Duration getStatusTtl() {
            return statusTtl;
        }

        public Duration getDataTtl() {
            return dataTtl;
        }
    }

    final class ReadProtocolRequest {
        private final String key;
        private final String dataKey;
        private final String statusKey;
        private final String validityKey;

        public ReadProtocolRequest(String key, String dataKey, String statusKey, String validityKey) {
            this.key = key;
            this.dataKey = dataKey;
            this.statusKey = statusKey;
            this.validityKey = validityKey;
        }

        public String getKey() {
            return key;
        }

        public String getDataKey() {
            return dataKey;
        }

        public String getStatusKey() {
            return statusKey;
        }

        public String getValidityKey() {
            return validityKey;
        }
    }

    final class ReadProtocolResult {
        private final BatchReadDebugSnapshot debugSnapshot;
        private final byte[] cachedValue;

        public ReadProtocolResult(BatchReadDebugSnapshot debugSnapshot, byte[] cachedValue) {
            this.debugSnapshot = debugSnapshot;
            this.cachedValue = cachedValue;
        }

        public BatchReadDebugSnapshot getDebugSnapshot() {
            return debugSnapshot;
        }

        public byte[] getCachedValue() {
            return cachedValue;
        }

        public BatchReadDebugSnapshot.Decision getDecision() {
            return debugSnapshot.getDecision();
        }

        public ProtocolState getState() {
            return debugSnapshot.getState();
        }

        public ProtocolValidity getValidity() {
            return debugSnapshot.getValidity();
        }
    }
}
