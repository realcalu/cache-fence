package io.github.cacheconsistency.core;

/**
 * Store-side operation that advances only the version counter to neutralize ghost writes.
 */
public interface VersionHealingOperation {
    void healVersion(String key, ConsistencyContext context);
}
