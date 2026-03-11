package io.github.cacheconsistency.example.api;

public class UserProfileBatchDeleteItem {
    private String userId;
    private String expectedVersion;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getExpectedVersion() {
        return expectedVersion;
    }

    public void setExpectedVersion(String expectedVersion) {
        this.expectedVersion = expectedVersion;
    }
}
