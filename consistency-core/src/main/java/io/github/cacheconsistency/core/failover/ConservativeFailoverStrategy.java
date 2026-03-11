package io.github.cacheconsistency.core.failover;

/**
 * Default failover strategy that only allows the minimal safe fallbacks exposed by the client.
 */
public final class ConservativeFailoverStrategy implements FailoverStrategy {
    public static final ConservativeFailoverStrategy INSTANCE = new ConservativeFailoverStrategy();

    private ConservativeFailoverStrategy() {
    }
}
