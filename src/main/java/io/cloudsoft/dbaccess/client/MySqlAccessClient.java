package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.python.google.common.collect.ImmutableList;

public class MySqlAccessClient extends AbstractDatabaseAccessClient {
    
    private static final String CREATE_USER = "CREATE USER '%s'@'%s' IDENTIFIED BY '%s'";
    private static final String GRANT_PERMISSIONS = "GRANT SELECT ON %s.* TO '%s'@'%s';";

    public MySqlAccessClient(String endpoint, String adminUsername, String adminPassword, String database) {
        super(endpoint, adminUsername, adminPassword, database);
    }

    @Override
    protected List<String> getGrantPermissionsStatements(String username, String password) {
        return ImmutableList.of(
                String.format(GRANT_PERMISSIONS, getDatabase(), username, "localhost"),
                String.format(GRANT_PERMISSIONS, getDatabase(), username, "%"));
    }

    @Override
    protected List<String> getCreateUserStatements(String username, String password) {
        return ImmutableList.of(String.format(CREATE_USER, username, "%", password),
                String.format(CREATE_USER, username, "localhost", password) 
                );
    }
}
