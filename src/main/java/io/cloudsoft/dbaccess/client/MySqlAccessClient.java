package io.cloudsoft.dbaccess.client;

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;

import com.google.common.collect.ImmutableList;

public class MySqlAccessClient extends AbstractDatabaseAccessClient {
    
    private static final String CREATE_USER = "CREATE USER '%s'@'%s' IDENTIFIED BY '%s'";
    private static final String GRANT_SELECT_PERMISSIONS = "GRANT SELECT ON %s.* TO '%s'@'%s';";
    private static final String GRANT_DELETE_PERMISSIONS = "GRANT DELETE ON %s.* TO '%s'@'%s';";
    private static final String GRANT_INSERT_PERMISSIONS = "GRANT INSERT ON %s.* TO '%s'@'%s';";
    private static final String GRANT_UPDATE_PERMISSIONS = "GRANT UPDATE ON %s.* TO '%s'@'%s';";
    private static final String DROP_USER = "DROP USER '%s'@'%s';";

    public MySqlAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database, List<String> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, permissions);
    }
    public MySqlAccessClient(ConfigurationSupport config) {
        super(config);
    }

    @Override
    protected String getDriverClass() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    protected Collection<String> getGrantSelectPermissionStatements(String username) {
        return ImmutableList.of(
                String.format(GRANT_SELECT_PERMISSIONS, getDatabase(), username, "localhost"),
                String.format(GRANT_SELECT_PERMISSIONS, getDatabase(), username, "%"));
    }

    @Override
    protected Collection<String> getGrantDeletePermissionStatements(String username) {
        return ImmutableList.of(
                String.format(GRANT_DELETE_PERMISSIONS, getDatabase(), username, "localhost"),
                String.format(GRANT_DELETE_PERMISSIONS, getDatabase(), username, "%"));
    }

    @Override
    protected Collection<String> getGrantInsertPermissionStatements(String username) {
        return ImmutableList.of(
                String.format(GRANT_INSERT_PERMISSIONS, getDatabase(), username, "localhost"),
                String.format(GRANT_INSERT_PERMISSIONS, getDatabase(), username, "%"));
    }

    @Override
    protected Collection<String> getGrantUpdatePermissionStatements(String username) {
        return ImmutableList.of(
                String.format(GRANT_UPDATE_PERMISSIONS, getDatabase(), username, "localhost"),
                String.format(GRANT_UPDATE_PERMISSIONS, getDatabase(), username, "%"));
    }

    @Override
    protected List<String> getCreateUserStatements(String username, String password) {
        return ImmutableList.of(
                String.format(CREATE_USER, username, "%", password),
                String.format(CREATE_USER, username, "localhost", password));
    }

    @Override
    protected List<String> getDeleteUserStatements(String username) {
        return ImmutableList.of(
                String.format(DROP_USER, username, "%"),
                String.format(DROP_USER, username, "localhost"));
    }

}
