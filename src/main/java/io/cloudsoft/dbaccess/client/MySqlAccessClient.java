package io.cloudsoft.dbaccess.client;

import java.util.List;

import org.apache.brooklyn.api.objs.Configurable.ConfigurationSupport;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.text.Strings;

import io.cloudsoft.dbaccess.DatabaseAccessEntity.AccessModes;
import io.cloudsoft.dbaccess.DatabaseAccessEntity.Permissions;

public class MySqlAccessClient extends AbstractDatabaseAccessClient {
    
    public MySqlAccessClient(ConfigurationSupport config) {
        super(config);
    }
    
    public MySqlAccessClient(String protocolScheme, String host, String port, String adminUsername, String adminPassword,
            String database, AccessModes accessMode, String accessScript, List<Permissions> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, accessMode, accessScript, permissions);
    }

    @Deprecated
    public MySqlAccessClient(String protocolScheme, String host, String port, 
            String adminUsername, String adminPassword, String database, List<String> permissions) {
        super(protocolScheme, host, port, adminUsername, adminPassword, database, permissions);
    }

    @Override
    protected String getDriverClass() {
        return "com.mysql.jdbc.Driver";
    }

    UserCommandsConnection newUserCommandsConnection(String description, String username, String password) {
        return new UserCommandsConnection(description, username, password) {
            protected List<String> expandHosts(String base) {
                return MutableList.of(
                    Strings.replaceAllNonRegex(base, "__HOST__", "localhost"),
                    Strings.replaceAllNonRegex(base, "__HOST__", "%"));
                
            }
            
            @Override
            protected List<String> getCreateUserStatements() {
                return expandHosts("CREATE USER '${user}'@'__HOST__' IDENTIFIED BY '${pass}'");
            }

            protected List<String> getGrantPermissionStatements(String permission) {
                return expandHosts("GRANT "+permission+" ON ${db}.* TO '${user}'@'__HOST__';");
            }
            
            @Override
            protected List<String> getDeleteUserStatements() {
                return expandHosts("DROP USER '${user}'@'__HOST__';");
            }
        };
    }

}
