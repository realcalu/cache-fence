package io.github.cacheconsistency.compensation;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileCompensationTaskStoreTest {
    @Test
    void shouldPersistAndReloadPendingTasks() throws Exception {
        Path file = Files.createTempFile("cck-compensation", ".log");
        try {
            FileCompensationTaskStore store = new FileCompensationTaskStore(file);
            CompensationTaskStore.CompensationTask task =
                    CompensationTaskStore.CompensationTask.write("cache:key", "value".getBytes("UTF-8"), Duration.ofSeconds(10));
            store.save(task);

            FileCompensationTaskStore reloaded = new FileCompensationTaskStore(file);
            List<CompensationTaskStore.CompensationTask> tasks = reloaded.loadPending();

            assertEquals(1, tasks.size());
            assertEquals("cache:key", tasks.get(0).getCacheKey());
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
