package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.collections.MutableList;

public class PostgresAccessClient extends AbstractDatabaseAccessClient {

    @Deprecated
    public PostgresAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database, List<String> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, permissions);
    }
    public PostgresAccessClient(ConfigurationSupport config) {
        super(config);
    }

    @Override
    protected String getDriverClass() {
        return "org.postgresql.Driver";
    }
    
    @Override
    protected String getJdbcUrlProtocolScheme() { return "jdbc:"+"postgresql"; }

    UserCommandsConnection newUserCommandsConnection(String description, String username, String password) {
        return new UserCommandsConnection(description, username, password) {
            
            @Override
            protected List<String> getCreateUserStatements() {
                return MutableList.of("CREATE USER ${user} WITH ENCRYPTED PASSWORD '${pass}'");
            }

            protected List<String> getGrantPermissionStatements(String permission) {
                return MutableList.of("GRANT "+permission+" ON ALL TABLES IN SCHEMA public"
                    +" TO ${user};");
            }
            
            @Override
            protected List<String> getDeleteUserStatements() {
                return MutableList.of(
                    "DROP OWNED BY ${user}",
                    "DROP USER ${user}");
            }
        };
    }
        
}
