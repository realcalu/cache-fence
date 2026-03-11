package io.github.cacheconsistency.example.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cacheconsistency.core.BatchPersistentOperation;
import io.github.cacheconsistency.core.ConsistencyContext;
import io.github.cacheconsistency.core.PersistentOperation;
import io.github.cacheconsistency.core.TransactionalPersistentOperation;
import io.github.cacheconsistency.example.model.UserProfile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * MySQL-backed store adapter used by the demo application.
 */
@Component
public class UserProfilePersistentOperation implements TransactionalPersistentOperation<UserProfile>, BatchPersistentOperation<UserProfile> {
    private final UserProfileStore store;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;

    public UserProfilePersistentOperation(UserProfileStore store,
                                         TransactionTemplate transactionTemplate,
                                         ObjectMapper objectMapper) {
        this.store = store;
        this.transactionTemplate = transactionTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public OperationResult<Void> update(ConsistencyContext context) {
        // SQL-level version checks remain mandatory even though the client also prechecks versions.
        int updated = store.update(context.getKey(), toJson(context.<UserProfile>getValue()), expectedVersion(context));
        return updated == 1 ? OperationResult.success() : OperationResult.versionRejected();
    }

    @Override
    public OperationResult<Void> delete(ConsistencyContext context) {
        int deleted = store.delete(context.getKey(), expectedVersion(context));
        return deleted == 1 ? OperationResult.success() : OperationResult.versionRejected();
    }

    @Override
    public OperationResult<String> queryVersion(ConsistencyContext context) {
        UserProfileRecord record = store.find(context.getKey());
        if (record == null) {
            return OperationResult.versionMissing();
        }
        return OperationResult.success(String.valueOf(record.getVersion()));
    }

    @Override
    public OperationResult<UserProfile> read(ConsistencyContext context) {
        UserProfileRecord record = store.find(context.getKey());
        return OperationResult.success(record == null ? null : fromJson(record.getProfileJson()));
    }

    @Override
    public Map<String, OperationResult<UserProfile>> readAll(Collection<ConsistencyContext> contexts) {
        Map<String, UserProfileRecord> records = store.findAll(keysOf(contexts));
        Map<String, OperationResult<UserProfile>> result = new LinkedHashMap<String, OperationResult<UserProfile>>();
        for (ConsistencyContext context : contexts) {
            UserProfileRecord record = records.get(context.getKey());
            result.put(context.getKey(), OperationResult.success(record == null ? null : fromJson(record.getProfileJson())));
        }
        return result;
    }

    @Override
    public Map<String, OperationResult<String>> queryVersionAll(Collection<ConsistencyContext> contexts) {
        Map<String, UserProfileRecord> records = store.findAll(keysOf(contexts));
        Map<String, OperationResult<String>> result = new LinkedHashMap<String, OperationResult<String>>();
        for (ConsistencyContext context : contexts) {
            UserProfileRecord record = records.get(context.getKey());
            result.put(context.getKey(), record == null
                    ? OperationResult.<String>versionMissing()
                    : OperationResult.success(String.valueOf(record.getVersion())));
        }
        return result;
    }

    @Override
    public Map<String, OperationResult<Void>> updateAll(Collection<ConsistencyContext> contexts) {
        Map<String, UserProfileStore.UpdateCommand> updates = new LinkedHashMap<String, UserProfileStore.UpdateCommand>();
        for (ConsistencyContext context : contexts) {
            updates.put(context.getKey(),
                    new UserProfileStore.UpdateCommand(toJson(context.<UserProfile>getValue()), expectedVersion(context)));
        }
        Map<String, Integer> updated = store.updateAll(updates);
        Map<String, OperationResult<Void>> result = new LinkedHashMap<String, OperationResult<Void>>();
        for (ConsistencyContext context : contexts) {
            result.put(context.getKey(), Integer.valueOf(1).equals(updated.get(context.getKey()))
                    ? OperationResult.<Void>success()
                    : OperationResult.<Void>versionRejected());
        }
        return result;
    }

    @Override
    public Map<String, OperationResult<Void>> deleteAll(Collection<ConsistencyContext> contexts) {
        Map<String, Long> deletes = new LinkedHashMap<String, Long>();
        for (ConsistencyContext context : contexts) {
            deletes.put(context.getKey(), Long.valueOf(expectedVersion(context)));
        }
        Map<String, Integer> deleted = store.deleteAll(deletes);
        Map<String, OperationResult<Void>> result = new LinkedHashMap<String, OperationResult<Void>>();
        for (ConsistencyContext context : contexts) {
            result.put(context.getKey(), Integer.valueOf(1).equals(deleted.get(context.getKey()))
                    ? OperationResult.<Void>success()
                    : OperationResult.<Void>versionRejected());
        }
        return result;
    }

    @Override
    public <R> R executeInTransaction(ConsistencyContext context, TransactionCallback<R> callback) {
        return transactionTemplate.execute(status -> callback.doInTransaction());
    }

    private long expectedVersion(ConsistencyContext context) {
        return Long.parseLong(context.getVersion());
    }

    private String toJson(UserProfile profile) {
        try {
            return objectMapper.writeValueAsString(profile);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to serialize user profile", error);
        }
    }

    private UserProfile fromJson(String json) {
        try {
            return objectMapper.readValue(json, UserProfile.class);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to deserialize user profile", error);
        }
    }

    private Collection<String> keysOf(Collection<ConsistencyContext> contexts) {
        Collection<String> keys = new java.util.ArrayList<String>(contexts.size());
        for (ConsistencyContext context : contexts) {
            keys.add(context.getKey());
        }
        return keys;
    }
}
