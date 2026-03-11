package io.github.cacheconsistency.example.api;

import io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory debug journal for recent batch write/delete protocol traces.
 */
@Component
public class ProtocolDebugJournal {
    private static final int DEFAULT_HISTORY_LIMIT = 10;

    private final Map<String, Deque<ProtocolWriteDebugRecord>> writeDebugHistory =
            new ConcurrentHashMap<String, Deque<ProtocolWriteDebugRecord>>();
    private final AtomicLong sequence = new AtomicLong(1L);

    public void recordBatch(Map<String, BatchWriteDebugSnapshot> snapshots) {
        long recordedAt = System.currentTimeMillis();
        for (Map.Entry<String, BatchWriteDebugSnapshot> entry : snapshots.entrySet()) {
            Deque<ProtocolWriteDebugRecord> records = history(entry.getKey());
            synchronized (records) {
                records.addFirst(new ProtocolWriteDebugRecord(
                        sequence.getAndIncrement(),
                        recordedAt,
                        entry.getValue()
                ));
                while (records.size() > DEFAULT_HISTORY_LIMIT) {
                    records.removeLast();
                }
            }
        }
    }

    public BatchWriteDebugSnapshot latestWrite(String key) {
        ProtocolWriteDebugRecord record = latestWriteRecord(key);
        return record == null ? null : record.getSnapshot();
    }

    public ProtocolWriteDebugRecord latestWriteRecord(String key) {
        Deque<ProtocolWriteDebugRecord> records = writeDebugHistory.get(key);
        if (records == null) {
            return null;
        }
        synchronized (records) {
            return records.peekFirst();
        }
    }

    public Map<String, BatchWriteDebugSnapshot> latestWrites(Collection<String> keys) {
        Map<String, BatchWriteDebugSnapshot> result = new LinkedHashMap<String, BatchWriteDebugSnapshot>();
        for (String key : keys) {
            result.put(key, latestWrite(key));
        }
        return result;
    }

    public List<ProtocolWriteDebugRecord> recentWrites(String key, int limit) {
        Deque<ProtocolWriteDebugRecord> records = writeDebugHistory.get(key);
        if (records == null) {
            return java.util.Collections.emptyList();
        }
        synchronized (records) {
            List<ProtocolWriteDebugRecord> result = new ArrayList<ProtocolWriteDebugRecord>(Math.min(limit, records.size()));
            int remaining = normalizeLimit(limit);
            for (ProtocolWriteDebugRecord record : records) {
                if (remaining-- <= 0) {
                    break;
                }
                result.add(record);
            }
            return result;
        }
    }

    public Map<String, List<ProtocolWriteDebugRecord>> recentWrites(Collection<String> keys, int limit) {
        Map<String, List<ProtocolWriteDebugRecord>> result =
                new LinkedHashMap<String, List<ProtocolWriteDebugRecord>>();
        for (String key : keys) {
            result.put(key, recentWrites(key, limit));
        }
        return result;
    }

    private Deque<ProtocolWriteDebugRecord> history(String key) {
        Deque<ProtocolWriteDebugRecord> records = writeDebugHistory.get(key);
        if (records != null) {
            return records;
        }
        Deque<ProtocolWriteDebugRecord> created = new ArrayDeque<ProtocolWriteDebugRecord>();
        Deque<ProtocolWriteDebugRecord> existing = writeDebugHistory.putIfAbsent(key, created);
        return existing == null ? created : existing;
    }

    private int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_HISTORY_LIMIT;
        }
        return Math.min(limit, DEFAULT_HISTORY_LIMIT);
    }
}
