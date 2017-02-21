package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.Strings;


public class OracleAccessClient extends AbstractDatabaseAccessClient {
    
    private static final String KILL_SESSIONS =
            "BEGIN\n" +
            "   FOR conn IN (SELECT sid, serial# FROM v$session WHERE username = UPPER('${user}')) LOOP\n" +
            "       EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || conn.sid || ',' || conn.serial# || '''';\n" +
            "   END LOOP;\n" +
            "END;\n";

    @Deprecated
    public OracleAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database, List<String> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, permissions);
    }
    public OracleAccessClient(ConfigurationSupport config) {
        super(config);
    }
    
    UserCommandsConnection newUserCommandsConnection(String description, String username, String password) {
        return new UserCommandsConnection(description, username, password) {
            
            @Override
            protected List<String> getCreateUserStatements() {
                return MutableList.of("CREATE USER ${user} IDENTIFIED BY ${pass}");
            }

            @Override
            protected List<String> getGrantSelectPermissionStatements() {
                return MutableList.of(
                    "GRANT CREATE SESSION TO ${user}",
                    // above could be part of create user; doesn't really matter -- both that and this run
                    "GRANT SELECT ANY TABLE"
                    +", SELECT ANY DICTIONARY"
                    +" TO ${user}");
            }
            
            protected List<String> getGrantPermissionStatements(String permission) {
                return MutableList.of("GRANT "+permission+" ANY TABLE"
                    +" TO ${user}");
            }
            
            @Override
            protected List<String> getDeleteUserStatements() {
                return MutableList.of(
                  // It's not possible to drop a login that has active sessions, so first
                  // we need to kill the sessions. As the user is read-only, there are no
                  // data-consistency implications
                    KILL_SESSIONS,
                    "DROP USER ${user}");
            }
        };
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
