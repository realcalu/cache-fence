package io.github.cacheconsistency.core.protocol;

import io.github.cacheconsistency.core.CommandType;
import io.github.cacheconsistency.core.WriteResult;

/**
 * Structured debug view for one key processed by the batch write/delete protocol.
 */
public final class BatchWriteDebugSnapshot {
    public enum StepStatus {
        NOT_RUN,
        SUCCESS,
        FAILED,
        REJECTED,
        SKIPPED
    }

    private final String businessKey;
    private final CommandType commandType;
    private final boolean optimizedPath;
    private final StepStatus prepareStatus;
    private final StepStatus versionStatus;
    private final StepStatus storeStatus;
    private final StepStatus stageStatus;
    private final StepStatus finalizeStatus;
    private final boolean compensationScheduled;
    private final WriteResult.WriteStatus finalStatus;

    public BatchWriteDebugSnapshot(String businessKey,
                                   CommandType commandType,
                                   boolean optimizedPath,
                                   StepStatus prepareStatus,
                                   StepStatus versionStatus,
                                   StepStatus storeStatus,
                                   StepStatus stageStatus,
                                   StepStatus finalizeStatus,
                                   boolean compensationScheduled,
                                   WriteResult.WriteStatus finalStatus) {
        this.businessKey = businessKey;
        this.commandType = commandType;
        this.optimizedPath = optimizedPath;
        this.prepareStatus = prepareStatus;
        this.versionStatus = versionStatus;
        this.storeStatus = storeStatus;
        this.stageStatus = stageStatus;
        this.finalizeStatus = finalizeStatus;
        this.compensationScheduled = compensationScheduled;
        this.finalStatus = finalStatus;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    public boolean isOptimizedPath() {
        return optimizedPath;
    }

    public StepStatus getPrepareStatus() {
        return prepareStatus;
    }

    public StepStatus getVersionStatus() {
        return versionStatus;
    }

    public StepStatus getStoreStatus() {
        return storeStatus;
    }

    public StepStatus getStageStatus() {
        return stageStatus;
    }

    public StepStatus getFinalizeStatus() {
        return finalizeStatus;
    }

    public boolean isCompensationScheduled() {
        return compensationScheduled;
    }

    public WriteResult.WriteStatus getFinalStatus() {
        return finalStatus;
    }
}
