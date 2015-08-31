package io.cloudsoft.dbaccess.client;

public abstract class AbstractDatabaseAccessClient implements DatabaseAccessClient {

    private final String endpoint;
    private final String adminPassword;
    private final String adminUsername;
    private final String database;

    public AbstractDatabaseAccessClient(String endpoint, String adminUsername, String adminPassword, String database) {
        this.endpoint = endpoint;
        this.adminUsername = adminUsername;
        this.adminPassword = adminPassword;
        this.database = database;
    }

    protected String getEndpoint() {
        return endpoint;
    }

    protected String getAdminPassword() {
        return adminPassword;
    }

    protected String getAdminUsername() {
        return adminUsername;
    }

    public String getDatabase() {
        return database;
    }
}
