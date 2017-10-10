package io.cloudsoft.dbaccess.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Scanner;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.cloudsoft.dbaccess.DatabaseAccessEntity;
import io.cloudsoft.dbaccess.DatabaseAccessEntity.AccessModes;
import io.cloudsoft.dbaccess.DatabaseAccessEntity.Permission;

public abstract class AbstractDatabaseAccessClient implements DatabaseAccessClient {

    private final String protocolScheme;
    private final String host;
    private final String port;
    private final String adminPassword;
    private final String adminUsername;
    private final String database;
    private final AccessModes accessMode;
    private final String accessScript;
    private final List<Permission> permissions;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDatabaseAccessClient.class);

    public AbstractDatabaseAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, 
            @Nullable String database, 
            @Nullable AccessModes accessMode, 
            @Nullable String accessScript, 
            @Nullable List<Permission> permissions) {
        this.protocolScheme = Preconditions.checkNotNull(protocolScheme, "protocol scheme");
        this.host = Preconditions.checkNotNull(host, "host");
        this.port = Preconditions.checkNotNull(port, "port");
        this.adminUsername = Preconditions.checkNotNull(adminUsername, "admin username");
        this.adminPassword = Preconditions.checkNotNull(adminPassword, "admin password");
        this.database = database;
        this.accessMode = accessMode==null ? AccessModes.READ_ONLY : accessMode;
        this.accessScript = accessScript;
        this.permissions = MutableList.copyOf(toPermList(permissions)).asUnmodifiable();
    }

    @Deprecated
    public AbstractDatabaseAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, 
            @Nullable String database, 
            @Nullable List<String> permissions) {
        this(protocolScheme, host, port, adminUsername, adminPassword, database, 
            permissions!=null && !permissions.isEmpty() ? AccessModes.READ_WRITE : AccessModes.READ_ONLY, 
            null, toPermList(permissions));
    }
    
    private static List<Permission> toPermList(List<?> permissions) {
        List<Permission> result = MutableList.of();
        if (permissions==null) return result;
        for (Object p: permissions) {
            result.add(TypeCoercions.coerce(p, Permission.class));
        }
        return result;
    }

    public AbstractDatabaseAccessClient(ConfigurationSupport config) {
        this( 
            config.get(DatabaseAccessEntity.PROTOCOL_SCHEME), 
            config.get(DatabaseAccessEntity.HOST), 
            config.get(DatabaseAccessEntity.PORT), 
            config.get(DatabaseAccessEntity.ADMIN_USER), 
            config.get(DatabaseAccessEntity.ADMIN_PASSWORD), 
            config.get(DatabaseAccessEntity.DATABASE),
            config.get(DatabaseAccessEntity.ACCESS_MODE),
            config.get(DatabaseAccessEntity.ACCESS_SCRIPT),
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
    public String getAdminJdbcUrlForInfo() {
        return getJdbcUrl(getAdminUsername(), null);
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
        try (UserCommandsConnection uc = newUserCommandsConnection("creating user", username, password)) {
            uc.createUser();
            if (AccessModes.CUSTOM != accessMode) {
                uc.grantPermissions();
            }
            if (AccessModes.CUSTOM == accessMode) {
                uc.grantCustomAccessScript();
            }
        } catch (Exception e) {
            throw Exceptions.propagate("Error setting up new user '"+username+"'. "
                    + "Confirm admin credential and see logs for more information.", e);
        }
    }

    @Override
    public void deleteUser(String username) {
        try (UserCommandsConnection uc = newUserCommandsConnection("deleting user", username, null)) {
            uc.deleteUser();
        }
    }

    @Override
    public void execute(String command, String usernameToReplace, String passwordToReplace) {
        try (UserCommandsConnection uc = newUserCommandsConnection("custom", usernameToReplace, passwordToReplace)) {
            uc.runStatements("custom", MutableList.of(command));
        }
    }
    
    protected void checkDriverClass() {
        try {
            Class.forName(getDriverClass());
        } catch (Exception e) {
            Exceptions.propagate("Unable to load "+getDriverClass(), e);
        }
    }

    abstract UserCommandsConnection newUserCommandsConnection(String description, String username, String password);
    
    abstract class UserCommandsConnection implements AutoCloseable {
        
        final String username, password;
        Connection connection;
        
        public UserCommandsConnection(String description, String username, String password) {
            this.username = username;
            this.password = password;
            String jdbcUrl = getJdbcUrl(getAdminUsername(), getAdminPassword());
            LOG.debug("Connecting to " + jdbcUrl+" to manage "+username+": "+description);
            
            checkDriverClass();
            
            this.connection = getConnection(jdbcUrl);
        }
        
        @Override
        public void close() {
            if (connection!=null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    Exceptions.propagate(e);
                    LOG.warn("Error closing "+connection+" (ignoring): "+e, e);
                }
            }
        }

        public String templated(String s) {
            return TemplateProcessor.processTemplateContents(s, MutableMap.of(
                "db", getDatabase(),
                "user", username,
                "pass", password));
        }
        
        public List<String> parseSqlScript(String in) throws SQLException {
            // based on http://stackoverflow.com/questions/1497569/how-to-execute-sql-script-file-using-jdbc
            Scanner s = new Scanner(in);
            s.useDelimiter("(;(\r)?\n)|(--\n)");
            List<String> result = MutableList.of();
            while (s.hasNext()) {
                String line = s.next();
                if (line.startsWith("/*!") && line.endsWith("*/")) {
                    // unescape C-style comments
                    int i = line.indexOf(' ');
                    line = line.substring(i + 1, line.length() - " */".length());
                }
                if (!Strings.isBlank(line)) {
                    result.add(line);
                }
            }
            s.close();
            return result;
        }
        
        public void runStatements(String description, List<String> statements) {
            if (statements==null || statements.isEmpty()) return;
            try (Statement statement = connection.createStatement()) {
                for (String s : statements) {
                    for (String si: parseSqlScript(templated(s))) {
                        LOG.debug("Executing: "+si);
                        statement.execute(si);
                    }
                }
            } catch (SQLException e) {
                throw Exceptions.propagateAnnotated("Error executing SQL for "+description, e);
            }            
        }
        
        public void createUser() {
            runStatements("create user", getCreateUserStatements());
        }
    
        private void grantPermissions() {
            runStatements("grant permissions", getGrantPermissionsStatements());
        }
    
        private void grantCustomAccessScript() {
            if (Strings.isBlank(accessScript)) return;
            runStatements("grant custom access script", MutableList.<String>builder().addAll(accessScript.split("\n")).build());
        }
        
        private void deleteUser() {
            runStatements("delete user", getDeleteUserStatements());
        }
        
        protected List<String> getGrantPermissionsStatements() {
            List<String> permissionStatements = Lists.newArrayList();
            
            if (AccessModes.CUSTOM == accessMode) return permissionStatements;
            
            // at a minimum should have SELECT permission
            permissionStatements.addAll(getGrantSelectPermissionStatements());
            
            if (AccessModes.READ_ONLY == accessMode) {
                if (permissions != null && !permissionStatements.isEmpty()) {
                    LOG.warn("Read-only mode specified; ignoring non-empty requested permissions "+permissions);
                }
                return permissionStatements;
            }
            
            if (permissions == null || permissions.isEmpty()) {
                LOG.warn("Access mode "+accessMode+" specified but no permissions granted (beyond default SELECT)");
                return permissionStatements;
            }
    
            for (Permission permission : permissions) {
                switch (permission) {
                    case DELETE:
                        permissionStatements.addAll(getGrantDeletePermissionStatements());
                        break;
                    case INSERT:
                        permissionStatements.addAll(getGrantInsertPermissionStatements());
                        break;
                    case UPDATE:
                        permissionStatements.addAll(getGrantUpdatePermissionStatements());
                        break;
                }
            }
            return permissionStatements;
        }
    
        protected List<String> getGrantSelectPermissionStatements() {
            return getGrantPermissionStatementsDefault("SELECT");
        }

        protected List<String> getGrantDeletePermissionStatements() {
            return getGrantPermissionStatementsDefault("DELETE");
        }

        protected List<String> getGrantInsertPermissionStatements() {
            return getGrantPermissionStatementsDefault("INSERT");
        }

        protected List<String> getGrantUpdatePermissionStatements() {
            return getGrantPermissionStatementsDefault("UPDATE");
        }
    
        protected abstract List<String> getCreateUserStatements();

        protected abstract List<String> getGrantPermissionStatementsDefault(String permission);

        protected abstract List<String> getDeleteUserStatements();
    }
    
    protected abstract String getDriverClass();
    
    private Connection getConnection(String jdbcUrl){
        try {
            return DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw Exceptions.propagate((Throwable) e);
        }
    }
}
