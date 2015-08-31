package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DBAccessClient;
import io.cloudsoft.dbaccess.client.PostgresAccessClient;

public class PostgresDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements PostresDatabaseAccessEntity {
    @Override
    public DBAccessClient createClient() {
        return new PostgresAccessClient();
    }
}
