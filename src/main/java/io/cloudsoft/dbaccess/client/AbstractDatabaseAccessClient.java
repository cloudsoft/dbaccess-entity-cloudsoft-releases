package io.cloudsoft.dbaccess.client;

import io.cloudsoft.dbaccess.DatabaseAccessEntity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public abstract class AbstractDatabaseAccessClient implements DatabaseAccessClient {

    private final String protocolScheme;
    private final String host;
    private final String port;
    private final String adminPassword;
    private final String adminUsername;
    private final String database;
    private List<String> permissions;

    private static final Logger LOG = LoggerFactory.getLogger(PostgresAccessClient.class);

    public AbstractDatabaseAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, @Nullable String database, @Nullable List<String> permissions) {
        this.protocolScheme = Preconditions.checkNotNull(protocolScheme, "protocol scheme");
        this.host = Preconditions.checkNotNull(host, "host");
        this.port = Preconditions.checkNotNull(port, "port");
        this.adminUsername = Preconditions.checkNotNull(adminUsername, "admin username");
        this.adminPassword = Preconditions.checkNotNull(adminPassword, "admin password");
        this.database = database;
        this.permissions = permissions;
    }

    public AbstractDatabaseAccessClient(ConfigurationSupport config) {
        this( 
            config.get(DatabaseAccessEntity.PROTOCOL_SCHEME), 
            config.get(DatabaseAccessEntity.HOST), 
            config.get(DatabaseAccessEntity.PORT), 
            config.get(DatabaseAccessEntity.ADMIN_USER), 
            config.get(DatabaseAccessEntity.ADMIN_PASSWORD), 
            config.get(DatabaseAccessEntity.DATABASE),
            config.get(DatabaseAccessEntity.PERMISSIONS)
        );
    }

    protected String getProtocolScheme() {
        return protocolScheme;
    }
    
    protected String getHost() {
        return host;
    }
    
    protected String getPort() {
        return port;
    }
    
    protected String getAdminUsername() {
        return adminUsername;
    }
    
    protected String getAdminPassword() {
        return adminPassword;
    }

    public String getDatabase() {
        return database;
    }
    
    @Override
    public String getJdbcUrl(String user, String pass) {
        StringBuilder jdbcUrl = new StringBuilder();
        jdbcUrl.append(getJdbcUrlProtocolScheme());
        jdbcUrl.append("://");
        jdbcUrl.append(getHost());
        if (Strings.isNonBlank(getPort())) jdbcUrl.append(":"+getPort());
        if (Strings.isNonBlank(getDatabase())) jdbcUrl.append(getJdbcUrlDatabaseSegmentWithPrefix());
        if (user!=null) {
            jdbcUrl.append(getJdbcUrlPropertiesSeparatorBefore());
            jdbcUrl.append("user="+Urls.encode(user));
            if (pass!=null) {
                jdbcUrl.append(getJdbcUrlPropertiesSeparatorBetween());
                jdbcUrl.append("password="+Urls.encode(pass));
            }
        }
        return jdbcUrl.toString();
    }
    
    protected String getJdbcUrlProtocolScheme() { return "jdbc:"+getProtocolScheme(); }
    protected String getJdbcUrlPropertiesSeparatorBefore() { return "?"; }
    protected String getJdbcUrlPropertiesSeparatorBetween() { return "&"; }
    protected String getJdbcUrlDatabaseSegmentWithPrefix() { return "/"+getDatabase(); }

    @Override
    public void createUser(String username, String password) {
        try {
            Class.forName(getDriverClass());
        } catch (ClassNotFoundException e) {
            Exceptions.propagateIfFatal(e);
        }
        String jdbcUrl = getJdbcUrl(getAdminUsername(), getAdminPassword());
        LOG.info("Connecting to " + jdbcUrl+" to create "+username);

        Connection connection = getConnection(jdbcUrl);
        try {
            createUser(connection, username, password);
            if (!grantPermissions(connection, username, password))
                throw new IllegalStateException("Unable to create user '"+username+"'. Confirm admin credential and see logs for more information.");
        } finally {
            close(connection);
        }
    }

    @Override
    public void deleteUser(String username) {
        try {
            Class.forName(getDriverClass());
        } catch (ClassNotFoundException e) {
            Exceptions.propagateIfFatal(e);
        }

        String jdbcUrl = getJdbcUrl(getAdminUsername(), getAdminPassword());
        LOG.info("Connecting to " + jdbcUrl+" to delete "+username);

        Connection connection = getConnection(jdbcUrl);
        deleteUser(connection, username);
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

    private boolean grantPermissions(Connection connection, String username, String password) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String grantStatement : getGrantPermissionsStatements(username, password)){
                statement.execute(grantStatement);
            }
            return true;
        } catch (SQLException e) {
            LOG.error("error executing SQL for grantPermissions: {}", e);
            Exceptions.propagateIfFatal(e);
            return false;
        } finally {
            close(statement);
        }
    }

    private boolean deleteUser(Connection connection, String username) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            for (String deleteUserStatement : getDeleteUserStatements(username)){
                statement.execute(deleteUserStatement);
            }
            return true;
        } catch (SQLException e) {
            LOG.error("error executing SQL for deleteUser: {}", e);
            Exceptions.propagateIfFatal(e);
            return false;
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

    protected List<String> getGrantPermissionsStatements(String username, String password) {
        List<String> permissionStatements = Lists.newArrayList();
        // at a minimum should have SELECT permission
        permissionStatements.addAll(getGrantSelectPermissionStatements(username));
        if (permissions == null) return permissionStatements;

        for (String permission : permissions) {
            switch (permission.toUpperCase()) {
                case "DELETE":
                    permissionStatements.addAll(getGrantDeletePermissionStatements(username));
                    break;
                case "INSERT":
                    permissionStatements.addAll(getGrantInsertPermissionStatements(username));
                    break;
                case "UPDATE":
                    permissionStatements.addAll(getGrantUpdatePermissionStatements(username));
                    break;
            }
        }
        return permissionStatements;
    }

    protected abstract Collection<String> getGrantSelectPermissionStatements(String username);
    protected abstract Collection<String> getGrantDeletePermissionStatements(String username);
    protected abstract Collection<String> getGrantInsertPermissionStatements(String username);
    protected abstract Collection<String> getGrantUpdatePermissionStatements(String username);

    protected abstract List<String> getDeleteUserStatements(String username);

    protected abstract String getDriverClass();

    private Connection getConnection(String jdbcUrl){
        try {
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw Exceptions.propagate(e);
        }
    }
}
