package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.net.Urls;

import com.google.common.collect.ImmutableList;

public class MsSqlAccessClient extends AbstractDatabaseAccessClient {

    private static final String CREATE_LOGIN = "CREATE LOGIN %s WITH PASSWORD = '%s', CHECK_POLICY = OFF";
    private static final String CREATE_USER = "CREATE USER %s FOR LOGIN %s";
    private static final String ADD_READONLY_ROLE = "ALTER ROLE db_datareader ADD MEMBER %s";
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
            String adminUsername, String adminPassword, String database) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database);
    }
    public MsSqlAccessClient(ConfigurationSupport config) {
        super(config);
    }

    @Override
    protected String getDriverClass() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    protected List<String> getGrantPermissionsStatements(String username, String password) {
        return ImmutableList.of(String.format(ADD_READONLY_ROLE, username));
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
