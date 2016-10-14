package io.cloudsoft.dbaccess.client;

import java.util.Collection;
import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.collect.ImmutableList;


public class OracleAccessClient extends AbstractDatabaseAccessClient {
    
    
    private static final String CREATE_USER = "CREATE USER %s IDENTIFIED BY %s";
    private static final String GRANT_CREATE_SESSION = "GRANT CREATE SESSION TO %s";
    private static final String GRANT_SELECT_PERMISSIONS = "GRANT SELECT ANY TABLE, SELECT ANY DICTIONARY TO %s";
    private static final String GRANT_DELETE_PERMISSIONS = "GRANT DELETE ANY TABLE TO %s";
    private static final String GRANT_INSERT_PERMISSIONS = "GRANT INSERT ANY TABLE TO %s";
    private static final String GRANT_UPDATE_PERMISSIONS = "GRANT UPDATE ANY TABLE TO %s";
    private static final String KILL_SESSIONS =
            "BEGIN\n" +
            "   FOR conn IN (SELECT sid, serial# FROM v$session WHERE username = UPPER('%s')) LOOP\n" +
            "       EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || conn.sid || ',' || conn.serial# || '''';\n" +
            "   END LOOP;\n" +
            "END;\n";
    private static final String DROP_USER = "DROP USER %s";

    public OracleAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database, List<String> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, permissions);
    }
    public OracleAccessClient(ConfigurationSupport config) {
        super(config);
    }
    
    @Override
    protected List<String> getCreateUserStatements(String username, String password) {
        return ImmutableList.of(String.format(CREATE_USER, username, password));
    }

    @Override
    protected Collection<String> getGrantSelectPermissionStatements(String username) {
        return ImmutableList.of(
                String.format(GRANT_CREATE_SESSION, username),
                String.format(GRANT_SELECT_PERMISSIONS, username)
        );
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
                // It's not possible to drop a login that has active sessions, so first
                // we need to kill the sessions. As the user is read-only, there are no
                // data-consistency implications
                String.format(KILL_SESSIONS, username),
                String.format(DROP_USER, username)
        );
    }

    @Override
    protected String getDriverClass() {
        return "oracle.jdbc.OracleDriver";
    }
    
    @Override
    public String getJdbcUrl(String user, String pass) {
        StringBuilder jdbcUrl = new StringBuilder();
        // http://stackoverflow.com/questions/1054105/url-string-format-for-connecting-to-oracle-database-with-jdbc
//        jdbc:oracle:thin:[USER/PASSWORD]@[HOST][:PORT]:SID
//        jdbc:oracle:thin:[USER/PASSWORD]@//[HOST][:PORT]/SERVICE
        jdbcUrl.append(getJdbcUrlProtocolScheme());
        jdbcUrl.append(":");
        if (user!=null) {
            jdbcUrl.append(Urls.encode(user));
            jdbcUrl.append("/");
            if (pass!=null) {
                jdbcUrl.append(Urls.encode(pass));
            }
        }
        jdbcUrl.append("@");
        jdbcUrl.append(getHost());
        if (Strings.isNonBlank(getPort())) jdbcUrl.append(":"+getPort());
        if (Strings.isNonBlank(getDatabase())) jdbcUrl.append(getJdbcUrlDatabaseSegmentWithPrefix());
        return jdbcUrl.toString();
    }
    @Override protected String getJdbcUrlProtocolScheme() { return "jdbc:oracle:thin"; }
    // assume SID format
    @Override protected String getJdbcUrlDatabaseSegmentWithPrefix() { return ":"+getDatabase(); }

}
