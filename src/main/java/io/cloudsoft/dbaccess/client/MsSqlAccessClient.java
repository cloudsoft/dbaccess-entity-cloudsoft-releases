package io.cloudsoft.dbaccess.client;

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.net.Urls;

import com.google.common.collect.ImmutableList;

public class MsSqlAccessClient extends AbstractDatabaseAccessClient {

    private static final String CREATE_LOGIN = "CREATE LOGIN %s WITH PASSWORD = '%s', CHECK_POLICY = OFF";
    private static final String CREATE_USER = "CREATE USER %s FOR LOGIN %s";
    private static final String GRANT_SELECT_PERMISSIONS = "GRANT SELECT TO %s";
    private static final String GRANT_DELETE_PERMISSIONS = "GRANT DELETE TO %s";
    private static final String GRANT_INSERT_PERMISSIONS = "GRANT INSERT TO %s";
    private static final String GRANT_UPDATE_PERMISSIONS = "GRANT UPDATE TO %s";
    private static final String KILL_SESSIONS =
            "DECLARE @loginNameToDrop sysname\r\n" +
            "SET @loginNameToDrop = '%s';\r\n" +

            "DECLARE sessionsToKill CURSOR FAST_FORWARD FOR\r\n" +
            "    SELECT session_id\r\n" +
            "    FROM sys.dm_exec_sessions\r\n" +
            "    WHERE login_name = @loginNameToDrop\r\n" +

            "OPEN sessionsToKill\r\n" +

            "DECLARE @sessionId INT\r\n" +
            "DECLARE @statement NVARCHAR(200)\r\n" +

            "FETCH NEXT FROM sessionsToKill INTO @sessionId\r\n" +

            "WHILE @@FETCH_STATUS = 0\r\n" +
            "BEGIN\r\n" +
            "    SET @statement = 'KILL ' + CAST(@sessionId AS NVARCHAR(20))\r\n" +
            "    EXEC sp_executesql @statement\r\n" +
            "    FETCH NEXT FROM sessionsToKill INTO @sessionId\r\n" +
            "END\r\n" +

            "CLOSE sessionsToKill\r\n" +
            "DEALLOCATE sessionsToKill";
    private static final String DROP_LOGIN = "DROP LOGIN %s";
    private static final String DROP_USER = "DROP USER %s";

    public MsSqlAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database, List<String> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, permissions);
    }
    public MsSqlAccessClient(ConfigurationSupport config) {
        super(config);
    }

    @Override
    protected String getDriverClass() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
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
    protected List<String> getCreateUserStatements(String username, String password) {
        return ImmutableList.of(
                String.format(CREATE_LOGIN, username, password),
                String.format(CREATE_USER, username, username)
        );
    }

    @Override
    protected List<String> getDeleteUserStatements(String username) {
        return ImmutableList.of(
                // It's not possible to drop a login that has active sessions, so first
                // we need to kill the sessions. As the user is read-only, there are no
                // data-consistency implications
                String.format(KILL_SESSIONS, username),
                String.format(DROP_USER, username),
                String.format(DROP_LOGIN, username)
        );
    }

    @Override protected String getJdbcUrlProtocolScheme() { return "jdbc:microsoft:sqlserver"; }
    @Override protected String getJdbcUrlPropertiesSeparatorBefore() { return ";"; }
    @Override protected String getJdbcUrlPropertiesSeparatorBetween() { return ";"; }
    @Override
    protected String getJdbcUrlDatabaseSegmentWithPrefix() {
        return getJdbcUrlPropertiesSeparatorBefore()+
            "databasename="+Urls.encode(getDatabase());
    }
    
}
