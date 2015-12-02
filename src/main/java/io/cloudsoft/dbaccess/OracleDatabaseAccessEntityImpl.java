package io.cloudsoft.dbaccess;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;
import io.cloudsoft.dbaccess.client.OracleAccessClient;

public class OracleDatabaseAccessEntityImpl extends DatabaseAccessEntityImpl implements OracleDatabaseAccessEntity{

    @Override
    public DatabaseAccessClient createClient() {
        return new OracleAccessClient(config());
    }
   
    @Override public String getProtocolScheme() { return "oracle"; }

}
