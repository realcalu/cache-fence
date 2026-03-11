package io.github.cacheconsistency.example.api;

import io.github.cacheconsistency.core.BatchDeleteCommand;
import io.github.cacheconsistency.core.BatchSetCommand;
import io.github.cacheconsistency.core.ConsistencyClient;
import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.ReadResult;
import io.github.cacheconsistency.core.WriteResult;
import io.github.cacheconsistency.core.protocol.BatchWriteDebugSnapshot;
import io.github.cacheconsistency.example.config.DemoRedisProperties;
import io.github.cacheconsistency.example.model.UserProfile;
import io.github.cacheconsistency.example.store.UserProfileStore;
import io.github.cacheconsistency.example.store.UserProfileRecord;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
public class UserProfileController {
    private final ConsistencyClient<UserProfile> consistencyClient;
    private final ProtocolDebugJournal protocolDebugJournal;
    private final UserProfileStore store;
    private final Duration cacheTtl;

    public UserProfileController(ConsistencyClient<UserProfile> consistencyClient,
                                 ProtocolDebugJournal protocolDebugJournal,
                                 UserProfileStore store,
                                 DemoRedisProperties properties) {
        this.consistencyClient = consistencyClient;
        this.protocolDebugJournal = protocolDebugJournal;
        this.store = store;
        this.cacheTtl = Duration.ofSeconds(properties.getCacheTtlSeconds());
    }

    @GetMapping("/api/users/{userId}")
    public UserProfileResponse get(@PathVariable("userId") String userId) {
        ReadResult<UserProfile> result = consistencyClient.get(userId, cacheTtl, ConsistencyContext.create());
        UserProfileRecord record = store.find(userId);
        return new UserProfileResponse(
                userId,
                result.getData(),
                record == null ? null : String.valueOf(record.getVersion()),
                result.getSource()
        );
    }

    @PutMapping("/api/users/{userId}")
    public Map<String, Object> put(@PathVariable("userId") String userId,
                                   @RequestBody UserProfileWriteRequest request) {
        UserProfile profile = new UserProfile(request.getName(), request.getEmail());
        WriteResult result = consistencyClient.set(
                userId,
                profile,
                cacheTtl,
                ConsistencyContext.create().withVersion(request.getExpectedVersion())
        );
        return response(userId, result);
    }

    @GetMapping("/api/users")
    public Map<String, UserProfileResponse> getBatch(@RequestParam("ids") String ids) {
        List<String> userIds = Arrays.asList(ids.split(","));
        Map<String, ReadResult<UserProfile>> readResults = consistencyClient.getAll(
                userIds,
                cacheTtl,
                ConsistencyContext.create()
        );
        Map<String, UserProfileRecord> records = store.findAll(userIds);
        Map<String, UserProfileResponse> body = new LinkedHashMap<String, UserProfileResponse>();
        for (String userId : userIds) {
            ReadResult<UserProfile> result = readResults.get(userId);
            UserProfileRecord record = records.get(userId);
            body.put(userId, new UserProfileResponse(
                    userId,
                    result == null ? null : result.getData(),
                    record == null ? null : String.valueOf(record.getVersion()),
                    result == null ? null : result.getSource()
            ));
        }
        return body;
    }

    @PutMapping("/api/users")
    public Map<String, Map<String, Object>> putBatch(@RequestBody List<UserProfileBatchWriteItem> requests) {
        List<BatchSetCommand<UserProfile>> commands = new java.util.ArrayList<BatchSetCommand<UserProfile>>(requests.size());
        for (UserProfileBatchWriteItem request : requests) {
            commands.add(BatchSetCommand.of(
                    request.getUserId(),
                    new UserProfile(request.getName(), request.getEmail()),
                    ConsistencyContext.create().withVersion(request.getExpectedVersion())
            ));
        }
        Map<String, BatchWriteDebugSnapshot> results = consistencyClient.setAllWithContextsDetailed(
                commands,
                cacheTtl,
                ConsistencyContext.create()
        );
        protocolDebugJournal.recordBatch(results);
        Map<String, Map<String, Object>> body = new LinkedHashMap<String, Map<String, Object>>();
        for (UserProfileBatchWriteItem request : requests) {
            body.put(request.getUserId(), response(request.getUserId(), results.get(request.getUserId())));
        }
        return body;
    }

    @DeleteMapping("/api/users/{userId}")
    @ResponseStatus(HttpStatus.OK)
    public Map<String, Object> delete(@PathVariable("userId") String userId,
                                      @RequestBody UserProfileWriteRequest request) {
        WriteResult result = consistencyClient.delete(
                userId,
                ConsistencyContext.create().withVersion(request.getExpectedVersion())
        );
        return response(userId, result);
    }

    @DeleteMapping("/api/users")
    @ResponseStatus(HttpStatus.OK)
    public Map<String, Map<String, Object>> deleteBatch(@RequestBody List<UserProfileBatchDeleteItem> requests) {
        List<BatchDeleteCommand> commands = new java.util.ArrayList<BatchDeleteCommand>(requests.size());
        for (UserProfileBatchDeleteItem request : requests) {
            commands.add(BatchDeleteCommand.of(
                    request.getUserId(),
                    ConsistencyContext.create().withVersion(request.getExpectedVersion())
            ));
        }
        Map<String, BatchWriteDebugSnapshot> results = consistencyClient.deleteAllWithContextsDetailed(
                commands,
                ConsistencyContext.create()
        );
        protocolDebugJournal.recordBatch(results);
        Map<String, Map<String, Object>> body = new LinkedHashMap<String, Map<String, Object>>();
        for (UserProfileBatchDeleteItem request : requests) {
            body.put(request.getUserId(), response(request.getUserId(), results.get(request.getUserId())));
        }
        return body;
    }

    private Map<String, Object> response(String userId, WriteResult result) {
        UserProfileRecord record = store.find(userId);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("userId", userId);
        body.put("status", result.getStatus().name());
        body.put("currentVersion", record == null ? null : String.valueOf(record.getVersion()));
        return body;
    }

    private Map<String, Object> response(String userId, BatchWriteDebugSnapshot debugSnapshot) {
        UserProfileRecord record = store.find(userId);
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("userId", userId);
        body.put("status", debugSnapshot.getFinalStatus().name());
        body.put("currentVersion", record == null ? null : String.valueOf(record.getVersion()));
        body.put("debug", debugSnapshot);
        return body;
    }
}
