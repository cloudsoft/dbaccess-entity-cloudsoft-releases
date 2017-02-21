package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.net.Urls;

import com.google.common.collect.ImmutableList;

public class MsSqlAccessClient extends AbstractDatabaseAccessClient {

    private static final String KILL_SESSIONS =
            "DECLARE @loginNameToDrop sysname\r\n" +
            "SET @loginNameToDrop = '${user}';\r\n" +

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

    @Deprecated
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

    UserCommandsConnection newUserCommandsConnection(String description, String username, String password) {
        return new UserCommandsConnection(description, username, password) {
            
            @Override
            protected List<String> getCreateUserStatements() {
                return MutableList.of(
                    "CREATE LOGIN ${user} WITH PASSWORD = '${pass}', CHECK_POLICY = OFF",
                    "CREATE USER ${user} FOR LOGIN ${user}");
            }
            
            @Override
            protected List<String> getGrantPermissionStatements(String permission) {
                return MutableList.of("GRANT "+permission+" TO ${user}");
            }
        
            @Override
            protected List<String> getDeleteUserStatements() {
                return ImmutableList.of(
                        // It's not possible to drop a login that has active sessions, so first
                        // we need to kill the sessions. As the user is read-only, there are no
                        // data-consistency implications
                        KILL_SESSIONS, 
                        "DROP LOGIN ${user}",
                        "DROP USER ${user}"
                );

            }
        };
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
