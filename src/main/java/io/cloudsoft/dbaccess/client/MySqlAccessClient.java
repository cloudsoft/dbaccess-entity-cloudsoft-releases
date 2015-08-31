package io.cloudsoft.dbaccess.client;

public class MySqlAccessClient extends AbstractDatabaseAccessClient {

    public MySqlAccessClient(String endpoint, String adminUsername, String adminPassword, String database) {
        super(endpoint, adminUsername, adminPassword, database);
    }

    @Override
    public void createUser(String username, String password) {

    }

}
