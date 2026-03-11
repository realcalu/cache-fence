package io.github.cacheconsistency.example.api;

import io.github.cacheconsistency.core.protocol.BatchReadDebugSnapshot;
import io.github.cacheconsistency.core.protocol.ProtocolDiagnosis;
import io.github.cacheconsistency.core.protocol.ProtocolSnapshot;
import io.github.cacheconsistency.core.support.ProtocolDiagnostician;
import io.github.cacheconsistency.core.support.ProtocolInspector;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
public class ProtocolController {
    private final ProtocolInspector protocolInspector;
    private final ProtocolDiagnostician protocolDiagnostician;
    private final ProtocolDebugJournal protocolDebugJournal;

    public ProtocolController(ProtocolInspector protocolInspector,
                              ProtocolDiagnostician protocolDiagnostician,
                              ProtocolDebugJournal protocolDebugJournal) {
        this.protocolInspector = protocolInspector;
        this.protocolDiagnostician = protocolDiagnostician;
        this.protocolDebugJournal = protocolDebugJournal;
    }

    @GetMapping("/api/protocol/{userId}")
    public ProtocolSnapshot protocol(@PathVariable("userId") String userId) {
        return protocolInspector.snapshot(userId);
    }

    @GetMapping("/api/protocol/{userId}/diagnosis")
    public ProtocolDiagnosis diagnosis(@PathVariable("userId") String userId) {
        return protocolDiagnostician.diagnose(userId);
    }

    @GetMapping("/api/protocol/{userId}/read-debug")
    public BatchReadDebugSnapshot readDebug(@PathVariable("userId") String userId) {
        return protocolInspector.explainRead(userId);
    }

    @GetMapping("/api/protocol/{userId}/write-debug")
    public io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot writeDebug(@PathVariable("userId") String userId) {
        return protocolDebugJournal.latestWrite(userId);
    }

    @GetMapping("/api/protocol/{userId}/write-debug/history")
    public List<ProtocolWriteDebugRecord> writeDebugHistory(@PathVariable("userId") String userId,
                                                            @RequestParam(value = "limit", required = false, defaultValue = "10") int limit) {
        return protocolDebugJournal.recentWrites(userId, limit);
    }

    @GetMapping("/api/protocol")
    public Map<String, ProtocolSnapshot> protocolBatch(@RequestParam("keys") String keys) {
        List<String> businessKeys = Arrays.asList(keys.split(","));
        return protocolInspector.snapshotAll(businessKeys);
    }

    @GetMapping("/api/protocol/diagnosis")
    public Map<String, ProtocolDiagnosis> diagnosisBatch(@RequestParam("keys") String keys) {
        List<String> businessKeys = Arrays.asList(keys.split(","));
        return protocolDiagnostician.diagnoseAll(businessKeys);
    }

    @GetMapping("/api/protocol/read-debug")
    public Map<String, BatchReadDebugSnapshot> readDebugBatch(@RequestParam("keys") String keys) {
        List<String> businessKeys = Arrays.asList(keys.split(","));
        return protocolInspector.explainReadAll(businessKeys);
    }

    @GetMapping("/api/protocol/write-debug")
    public Map<String, io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot> writeDebugBatch(@RequestParam("keys") String keys) {
        List<String> businessKeys = Arrays.asList(keys.split(","));
        return protocolDebugJournal.latestWrites(businessKeys);
    }

    @GetMapping("/api/protocol/write-debug/history")
    public Map<String, List<ProtocolWriteDebugRecord>> writeDebugHistoryBatch(@RequestParam("keys") String keys,
                                                                              @RequestParam(value = "limit", required = false, defaultValue = "10") int limit) {
        List<String> businessKeys = Arrays.asList(keys.split(","));
        return protocolDebugJournal.recentWrites(businessKeys, limit);
    }
}
