package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;
import io.cloudsoft.dbaccess.client.MySqlAccessClient;

public class MySqlDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements MySqlDatabaseAccessEntity {

    @Override
    public DatabaseAccessClient createClient() {
        return new MySqlAccessClient(config().get(ENDPOINT_URL), config().get(ADMIN_USER), config().get(ADMIN_PASSWORD),
                config().get(DATABASE));
    }

}
