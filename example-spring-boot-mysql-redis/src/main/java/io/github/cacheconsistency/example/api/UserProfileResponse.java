package io.github.cacheconsistency.example.api;

import io.github.cacheconsistency.core.ReadResult;
import io.github.cacheconsistency.example.model.UserProfile;

public class UserProfileResponse {
    private String userId;
    private UserProfile profile;
    private String version;
    private ReadResult.ReadSource source;

    public UserProfileResponse(String userId, UserProfile profile, String version, ReadResult.ReadSource source) {
        this.userId = userId;
        this.profile = profile;
        this.version = version;
        this.source = source;
    }

    public String getUserId() {
        return userId;
    }

    public UserProfile getProfile() {
        return profile;
    }

    public String getVersion() {
        return version;
    }

    public ReadResult.ReadSource getSource() {
        return source;
    }
}
