package io.github.cacheconsistency.compensation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * File-backed task store for optional compensation tasks.
 */
public class FileCompensationTaskStore implements CompensationTaskStore {
    private final Path path;

    public FileCompensationTaskStore(Path path) {
        this.path = path;
    }

    @Override
    public synchronized CompensationTask save(CompensationTask task) {
        Map<String, Record> records = loadRecords();
        records.put(task.getId(), Record.pending(task, null));
        flush(records);
        return task;
    }

    @Override
    public synchronized List<CompensationTask> loadPending() {
        Map<String, Record> records = loadRecords();
        List<CompensationTask> result = new ArrayList<CompensationTask>();
        for (Record record : records.values()) {
            if (!record.terminal && !record.success) {
                result.add(record.task);
            }
        }
        return result;
    }

    @Override
    public synchronized void markSuccess(String taskId) {
        Map<String, Record> records = loadRecords();
        Record record = records.get(taskId);
        if (record != null) {
            records.put(taskId, new Record(record.task, true, false, null));
            flush(records);
        }
    }

    @Override
    public synchronized void markFailure(String taskId, int attempt, String message, boolean terminal) {
        Map<String, Record> records = loadRecords();
        Record record = records.get(taskId);
        if (record != null) {
            records.put(taskId, new Record(record.task.withAttempt(attempt), false, terminal, message));
            flush(records);
        }
    }

    private Map<String, Record> loadRecords() {
        try {
            if (!Files.exists(path)) {
                return new LinkedHashMap<String, Record>();
            }
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            Map<String, Record> records = new LinkedHashMap<String, Record>();
            for (String line : lines) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                Record record = Record.parse(line);
                records.put(record.task.getId(), record);
            }
            return records;
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to load compensation store", exception);
        }
    }

    private void flush(Map<String, Record> records) {
        try {
            List<String> lines = new ArrayList<String>(records.size());
            for (Record record : records.values()) {
                lines.add(record.encode());
            }
            Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to flush compensation store", exception);
        }
    }

    private static final class Record {
        private final CompensationTask task;
        private final boolean success;
        private final boolean terminal;
        private final String message;

        private Record(CompensationTask task, boolean success, boolean terminal, String message) {
            this.task = task;
            this.success = success;
            this.terminal = terminal;
            this.message = message;
        }

        private static Record pending(CompensationTask task, String message) {
            return new Record(task, false, false, message);
        }

        private String encode() {
            String payload = task.getPayload() == null ? "" : Base64.getEncoder().encodeToString(task.getPayload());
            return task.getId() + "|" + task.getAction().name() + "|" + task.getCacheKey() + "|" + payload + "|"
                    + task.getTtlSeconds() + "|" + task.getAttempt() + "|" + success + "|" + terminal + "|"
                    + (message == null ? "" : message);
        }

        private static Record parse(String line) {
            String[] parts = line.split("\\|", -1);
            byte[] payload = parts[3].isEmpty() ? null : Base64.getDecoder().decode(parts[3]);
            CompensationTask task = new CompensationTask(
                    parts[0],
                    CompensationTask.Action.valueOf(parts[1]),
                    parts[2],
                    payload,
                    Long.parseLong(parts[4]),
                    Integer.parseInt(parts[5])
            );
            return new Record(task, Boolean.parseBoolean(parts[6]), Boolean.parseBoolean(parts[7]),
                    parts.length > 8 && !parts[8].isEmpty() ? parts[8] : null);
        }
    }
}
