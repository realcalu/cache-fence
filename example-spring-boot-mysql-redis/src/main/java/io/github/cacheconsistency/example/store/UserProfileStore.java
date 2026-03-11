package io.github.cacheconsistency.example.store;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@Repository
public class UserProfileStore {
    private final JdbcTemplate jdbcTemplate;

    public UserProfileStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public UserProfileRecord find(String userId) {
        List<UserProfileRecord> rows = jdbcTemplate.query(
                "select user_id, profile_json, key_version from user_profile where user_id = ?",
                (rs, rowNum) -> new UserProfileRecord(
                        rs.getString("user_id"),
                        rs.getString("profile_json"),
                        rs.getLong("key_version")
                ),
                userId
        );
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Map<String, UserProfileRecord> findAll(Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Collections.emptyMap();
        }
        StringJoiner placeholders = new StringJoiner(",", "(", ")");
        Object[] args = new Object[userIds.size()];
        int index = 0;
        for (String userId : userIds) {
            placeholders.add("?");
            args[index++] = userId;
        }
        List<UserProfileRecord> rows = jdbcTemplate.query(
                "select user_id, profile_json, key_version from user_profile where user_id in " + placeholders,
                (rs, rowNum) -> new UserProfileRecord(
                        rs.getString("user_id"),
                        rs.getString("profile_json"),
                        rs.getLong("key_version")
                ),
                args
        );
        Map<String, UserProfileRecord> result = new LinkedHashMap<String, UserProfileRecord>();
        for (String userId : userIds) {
            result.put(userId, null);
        }
        for (UserProfileRecord row : rows) {
            result.put(row.getUserId(), row);
        }
        return result;
    }

    public int update(String userId, String profileJson, long expectedVersion) {
        return jdbcTemplate.update(
                "update user_profile set profile_json = ?, key_version = key_version + 1 " +
                        "where user_id = ? and key_version = ?",
                profileJson,
                userId,
                expectedVersion
        );
    }

    public Map<String, Integer> updateAll(Map<String, UpdateCommand> updates) {
        if (updates == null || updates.isEmpty()) {
            return Collections.emptyMap();
        }
        List<Map.Entry<String, UpdateCommand>> orderedEntries = new java.util.ArrayList<Map.Entry<String, UpdateCommand>>(updates.entrySet());
        int[] counts = jdbcTemplate.batchUpdate(
                "update user_profile set profile_json = ?, key_version = key_version + 1 " +
                        "where user_id = ? and key_version = ?",
                new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        Map.Entry<String, UpdateCommand> entry = orderedEntries.get(i);
                        ps.setString(1, entry.getValue().profileJson);
                        ps.setString(2, entry.getKey());
                        ps.setLong(3, entry.getValue().expectedVersion);
                    }

                    @Override
                    public int getBatchSize() {
                        return orderedEntries.size();
                    }
                }
        );
        Map<String, Integer> result = new LinkedHashMap<String, Integer>();
        for (int i = 0; i < orderedEntries.size(); i++) {
            result.put(orderedEntries.get(i).getKey(), Integer.valueOf(counts[i]));
        }
        return result;
    }

    public int delete(String userId, long expectedVersion) {
        return jdbcTemplate.update(
                "delete from user_profile where user_id = ? and key_version = ?",
                userId,
                expectedVersion
        );
    }

    public Map<String, Integer> deleteAll(Map<String, Long> deletes) {
        if (deletes == null || deletes.isEmpty()) {
            return Collections.emptyMap();
        }
        List<Map.Entry<String, Long>> orderedEntries = new java.util.ArrayList<Map.Entry<String, Long>>(deletes.entrySet());
        int[] counts = jdbcTemplate.batchUpdate(
                "delete from user_profile where user_id = ? and key_version = ?",
                new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        Map.Entry<String, Long> entry = orderedEntries.get(i);
                        ps.setString(1, entry.getKey());
                        ps.setLong(2, entry.getValue().longValue());
                    }

                    @Override
                    public int getBatchSize() {
                        return orderedEntries.size();
                    }
                }
        );
        Map<String, Integer> result = new LinkedHashMap<String, Integer>();
        for (int i = 0; i < orderedEntries.size(); i++) {
            result.put(orderedEntries.get(i).getKey(), Integer.valueOf(counts[i]));
        }
        return result;
    }

    public int bumpVersion(String userId) {
        return jdbcTemplate.update(
                "update user_profile set key_version = key_version + 1 where user_id = ?",
                userId
        );
    }

    public static final class UpdateCommand {
        private final String profileJson;
        private final long expectedVersion;

        public UpdateCommand(String profileJson, long expectedVersion) {
            this.profileJson = profileJson;
            this.expectedVersion = expectedVersion;
        }
    }
}
