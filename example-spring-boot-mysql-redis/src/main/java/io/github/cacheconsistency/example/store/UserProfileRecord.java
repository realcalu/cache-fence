package io.github.cacheconsistency.example.store;

public class UserProfileRecord {
    private final String userId;
    private final String profileJson;
    private final long version;

    public UserProfileRecord(String userId, String profileJson, long version) {
        this.userId = userId;
        this.profileJson = profileJson;
        this.version = version;
    }

    public String getUserId() {
        return userId;
    }

    public String getProfileJson() {
        return profileJson;
    }

    public long getVersion() {
        return version;
    }
}
