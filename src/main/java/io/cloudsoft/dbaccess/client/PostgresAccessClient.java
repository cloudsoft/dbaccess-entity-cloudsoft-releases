package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class PostgresAccessClient extends AbstractDatabaseAccessClient {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresAccessClient.class);

    private static final String CREATE_USER = "CREATE USER %s WITH ENCRYPTED PASSWORD '%s'";
    private static final String GRANT_PERMISSIONS = "ALTER DEFAULT PRIVILEGES IN SCHEMA public\n" +
            "GRANT SELECT ON TABLES TO %s;";

    public PostgresAccessClient(String endpoint, String adminUsername, String adminPassword, String database) {
        super(endpoint, adminUsername, adminPassword, database);
    }

    @Override
    protected List<String> getCreateUserStatements(String username, String password) {
        return ImmutableList.of(String.format(CREATE_USER, username, password));
    }

    @Override
    protected List<String> getGrantPermissionsStatements(String username, String password) {
        return ImmutableList.of(String.format(GRANT_PERMISSIONS, username));
    }
}
