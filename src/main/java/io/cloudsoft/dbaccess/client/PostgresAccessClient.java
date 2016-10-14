package io.cloudsoft.dbaccess.client;

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;

import com.google.common.collect.ImmutableList;

public class PostgresAccessClient extends AbstractDatabaseAccessClient {

    private static final String CREATE_USER = "CREATE USER %s WITH ENCRYPTED PASSWORD '%s'";
    private static final String GRANT_SELECT_PERMISSIONS = "GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s;";
    private static final String GRANT_DELETE_PERMISSIONS = "GRANT DELETE ON ALL TABLES IN SCHEMA public TO %s;";
    private static final String GRANT_INSERT_PERMISSIONS = "GRANT INSERT ON ALL TABLES IN SCHEMA public TO %s;";
    private static final String GRANT_UPDATE_PERMISSIONS = "GRANT UPDATE ON ALL TABLES IN SCHEMA public TO %s;";
    private static final String DISOWN_USER = "DROP OWNED BY %s";
    private static final String DROP_USER = "DROP USER %s";

    public PostgresAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database, List<String> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, permissions);
    }
    public PostgresAccessClient(ConfigurationSupport config) {
        super(config);
    }

    @Override
    protected String getDriverClass() {
        return "org.postgresql.Driver";
    }

    @Override
    protected List<String> getCreateUserStatements(String username, String password) {
        return ImmutableList.of(String.format(CREATE_USER, username, password));
    }

    @Override
    protected Collection<String> getGrantSelectPermissionStatements(String username) {
        return ImmutableList.of(String.format(GRANT_SELECT_PERMISSIONS, username));
    }

    @Override
    protected Collection<String> getGrantDeletePermissionStatements(String username) {
        return ImmutableList.of(String.format(GRANT_DELETE_PERMISSIONS, username));
    }

    @Override
    protected Collection<String> getGrantInsertPermissionStatements(String username) {
        return ImmutableList.of(String.format(GRANT_INSERT_PERMISSIONS, username));
    }

    @Override
    protected Collection<String> getGrantUpdatePermissionStatements(String username) {
        return ImmutableList.of(String.format(GRANT_UPDATE_PERMISSIONS, username));
    }

    @Override
    protected List<String> getDeleteUserStatements(String username) {
        return ImmutableList.of(
                String.format(DISOWN_USER, username),
                String.format(DROP_USER, username)
        );
    }
    
    protected String getJdbcUrlProtocolScheme() { return "jdbc:"+"postgresql"; }
    
}
