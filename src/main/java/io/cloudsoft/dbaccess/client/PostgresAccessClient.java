package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;

import com.google.common.collect.ImmutableList;

public class PostgresAccessClient extends AbstractDatabaseAccessClient {

    private static final String CREATE_USER = "CREATE USER %s WITH ENCRYPTED PASSWORD '%s'";
    private static final String GRANT_PERMISSIONS = "GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s;";
    private static final String DISOWN_USER = "DROP OWNED BY %s";
    private static final String DROP_USER = "DROP USER %s";

    public PostgresAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database);
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
    protected List<String> getGrantPermissionsStatements(String username, String password) {
        return ImmutableList.of(String.format(GRANT_PERMISSIONS, username));
    }

    @Override
    protected List<String> getDeleteUserStatements(String username) {
        return ImmutableList.of(
                String.format(DISOWN_USER, username),
                String.format(DROP_USER, username)
        );
    }
}
