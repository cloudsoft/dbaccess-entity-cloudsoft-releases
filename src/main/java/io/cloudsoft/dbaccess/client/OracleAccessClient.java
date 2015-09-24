package io.cloudsoft.dbaccess.client;

import java.util.List;

import com.google.common.collect.ImmutableList;


public class OracleAccessClient extends AbstractDatabaseAccessClient {
    
    
    private static final String CREATE_USER = "CREATE USER %s IDENTIFIED BY %s";
    private static final String GRANT_PERMISSIONS = "GRANT create session, select any table, select any dictionary TO %s";
    private static final String DROP_USER = "DROP USER %s";

    public OracleAccessClient(String endpoint, String adminUsername,
            String adminPassword, String database) {
        super(endpoint, adminUsername, adminPassword, database);
    }
    
    @Override
    protected List<String> getCreateUserStatements(String username,
            String password) {
        return ImmutableList.of(String.format(CREATE_USER, username, password));
    }

    @Override
    protected List<String> getGrantPermissionsStatements(String username,
            String password) {
        return ImmutableList.of(String.format(GRANT_PERMISSIONS, username));
    }

    @Override
    protected List<String> getDeleteUserStatements(String username) {
        return ImmutableList.of(String.format(DROP_USER, username));
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
