package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;
import io.cloudsoft.dbaccess.client.OracleAccessClient;

public class OracleDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements OracleDatabaseAccessEntity{

    @Override
    public DatabaseAccessClient createClient() {
        return new OracleAccessClient(config().get(ENDPOINT_URL), config().get(ADMIN_USER), config().get(ADMIN_PASSWORD),
            config().get(DATABASE));
    }
   
    @Override
    protected String makeDatastoreUrl(String endpoint, String database, String username, String password) {
        return String.format("%s/%s@//%s/%s", username, password, endpoint, database);
    }

}
