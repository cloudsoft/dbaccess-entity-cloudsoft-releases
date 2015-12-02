package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;
import io.cloudsoft.dbaccess.client.MsSqlAccessClient;

public class MsSqlDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements MsSqlDatabaseAccessEntity{

    @Override
    public DatabaseAccessClient createClient() {
        return new MsSqlAccessClient(config());
    }

    @Override public String getProtocolScheme() { return "mssql"; }
    
}
