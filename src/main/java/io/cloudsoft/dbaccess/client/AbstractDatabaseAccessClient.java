package io.cloudsoft.dbaccess.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import brooklyn.util.exceptions.Exceptions;

public abstract class AbstractDatabaseAccessClient implements DatabaseAccessClient {

    private final String endpoint;
    private final String adminPassword;
    private final String adminUsername;
    private final String database;

    private static final Logger LOG = LoggerFactory.getLogger(PostgresAccessClient.class);

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

    @Override
    public void createUser(String username, String password) {

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            Exceptions.propagateIfFatal(e);
        }
        String jdbcUrl = String.format("jdbc:%s%s?user=%s&password=%s", getEndpoint(), getDatabase(), getAdminUsername(), getAdminPassword());
        LOG.info("Connecting to " + jdbcUrl);

        Connection connection = getConnection(jdbcUrl);
        createUser(connection, username, password);
        grantPermissions(connection, username, password);
        close(connection);
    }

    public void createUser(Connection connection, String username, String password) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String createUserStatement : getCreateUserStatements(username, password)) {
                statement.execute(createUserStatement);
            }
        } catch (SQLException e) {
            LOG.error("error executing SQL for createUser: {}", e);
            Exceptions.propagateIfFatal(e);
        } finally {
            close(statement);
        }
    }

    private void grantPermissions(Connection connection, String username, String password) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String grantStatement : getGrantPermissionsStatements(username, password)){
                statement.execute(grantStatement);
            }
        } catch (SQLException e) {
            LOG.error("error executing SQL for grantPermissions: {}", e);
            Exceptions.propagateIfFatal(e);
        } finally {
            close(statement);
        }
    }

    public void close(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
        }
    }

    public void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
        }
    }

    protected abstract List<String> getCreateUserStatements(String username, String password);

    protected abstract List<String> getGrantPermissionsStatements(String username, String password);

    private Connection getConnection(String jdbcUrl){
        try {
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw Exceptions.propagate(e);
        }
    }
}
