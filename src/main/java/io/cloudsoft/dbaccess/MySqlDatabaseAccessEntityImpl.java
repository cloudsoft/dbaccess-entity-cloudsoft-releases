package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DBAccessClient;
import io.cloudsoft.dbaccess.client.MySqlAccessClient;

public class MySqlDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements MySqlDatabaseAccessEntity {

    @Override
    public DBAccessClient createClient() {
        return new MySqlAccessClient();
    }
}
