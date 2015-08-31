package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;
import io.cloudsoft.dbaccess.client.MySqlAccessClient;

public class MySqlDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements MySqlDatabaseAccessEntity {

    @Override
    public DatabaseAccessClient createClient() {
        return new MySqlAccessClient(null, null, null, null);
    }

}
