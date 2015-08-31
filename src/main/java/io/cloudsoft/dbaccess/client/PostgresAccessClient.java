package io.cloudsoft.dbaccess.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresAccessClient extends AbstractDatabaseAccessClient {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresAccessClient.class);

    private static final String CREATE_USER = "CREATE USER %s WITH ENCRYPTED PASSWORD '%s'";
    private static final String GRANT_PERMISSIONS = "ALTER DEFAULT PRIVILEGES IN SCHEMA public\n" +
        "GRANT SELECT ON TABLES TO %s;";

    public PostgresAccessClient(String endpoint, String adminUsername, String adminPassword, String database) {
        super(endpoint, adminUsername, adminPassword, database);
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
        grantPermissions(connection, username);
        close(connection);
    }

    
    public void createUser(Connection connection, String username, String password) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(String.format(CREATE_USER, username, password));
        } catch (SQLException e) {
            LOG.error("error executing SQL for createUser: {}", e);
            Exceptions.propagateIfFatal(e);
        } finally {
            close(statement);
        }
    }

    private void grantPermissions(Connection connection, String username) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(String.format(GRANT_PERMISSIONS, username));
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

    private Connection getConnection(String jdbcUrl){
        try {
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw Exceptions.propagate(e);
        }
    }

}
