package io.cloudsoft.dbaccess.client;

import java.util.List;

import com.google.common.collect.ImmutableList;


public class OracleAccessClient extends AbstractDatabaseAccessClient {
    
    
    private static final String CREATE_USER = "CREATE USER %s IDENTIFIED BY %s";
    private static final String GRANT_PERMISSIONS = "GRANT create session, select any table, select any dictionary TO %s";
    private static final String KILL_SESSIONS =
            "BEGIN\n" +
            "   FOR conn IN (SELECT sid, serial# FROM v$session WHERE username = UPPER('%s')) LOOP\n" +
            "       EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || conn.sid || ',' || conn.serial# || '''';\n" +
            "   END LOOP;\n" +
            "END;\n";
    private static final String DROP_USER = "DROP USER %s";

    public OracleAccessClient(String endpoint, String adminUsername, String adminPassword, String database) {
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
    public String connectionString() {
        return String.format("jdbc:oracle:thin:%s/%s@%s:%s", getAdminUsername(),  getAdminPassword(), getEndpoint(), getDatabase());
    }

}
