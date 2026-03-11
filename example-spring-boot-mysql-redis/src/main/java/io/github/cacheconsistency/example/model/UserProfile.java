package io.github.cacheconsistency.example.model;

public class UserProfile {
    private String name;
    private String email;

    public UserProfile() {
    }

    public UserProfile(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
