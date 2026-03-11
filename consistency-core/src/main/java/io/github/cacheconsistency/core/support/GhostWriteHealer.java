package io.github.cacheconsistency.core.support;

import io.github.cacheconsistency.core.ConsistencyContext;

public interface GhostWriteHealer {
    void scheduleHeal(String key, ConsistencyContext context);

    default void shutdown() {
    }

    final class NoOpGhostWriteHealer implements GhostWriteHealer {
        public static final NoOpGhostWriteHealer INSTANCE = new NoOpGhostWriteHealer();

        private NoOpGhostWriteHealer() {
        }

        @Override
        public void scheduleHeal(String key, ConsistencyContext context) {
        }
    }
}
