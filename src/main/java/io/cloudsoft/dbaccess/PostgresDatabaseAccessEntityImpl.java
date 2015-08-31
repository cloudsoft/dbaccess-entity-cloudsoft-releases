package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;
import io.cloudsoft.dbaccess.client.PostgresAccessClient;

public class PostgresDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements PostgresDatabaseAccessEntity {

    @Override
    public DatabaseAccessClient createClient() {
        return new PostgresAccessClient(config().get(ENDPOINT_URL), config().get(ADMIN_USER), config().get(ADMIN_PASSWORD),
            config().get(DATABASE));
    }

}
