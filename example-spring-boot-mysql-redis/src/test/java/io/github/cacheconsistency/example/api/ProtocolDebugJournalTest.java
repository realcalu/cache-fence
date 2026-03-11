package io.github.cacheconsistency.example.api;

import io.github.cacheconsistency.core.CommandType;
import io.github.cacheconsistency.core.WriteResult;
import io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtocolDebugJournalTest {
    @Test
    void shouldKeepRecentWriteDebugHistoryInMemory() {
        ProtocolDebugJournal journal = new ProtocolDebugJournal();
        for (int i = 0; i < 12; i++) {
            journal.recordBatch(Collections.singletonMap(
                    "user:1",
                    new BatchWriteDebugSnapshot(
                            "user:1",
                            CommandType.SET,
                            true,
                            BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                            BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                            BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                            BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                            BatchWriteDebugSnapshot.StepStatus.SUCCESS,
                            false,
                            WriteResult.WriteStatus.STORE_AND_CACHE_UPDATED
                    )));
        }

        List<ProtocolWriteDebugRecord> records = journal.recentWrites("user:1", 20);

        assertEquals(10, records.size());
        assertEquals(records.get(0).getSequence() - 9, records.get(9).getSequence());
    }
}
