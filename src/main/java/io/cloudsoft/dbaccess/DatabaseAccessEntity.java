package io.cloudsoft.dbaccess;

import java.util.List;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.BasicAttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.database.DatastoreMixins;
import org.apache.brooklyn.entity.stock.BasicApplication;

import com.google.common.base.CaseFormat;
import com.google.common.reflect.TypeToken;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;

public interface DatabaseAccessEntity extends BasicApplication, DatastoreMixins.HasDatastoreUrl {

    ConfigKey<String> ENDPOINT_URL = ConfigKeys.newStringConfigKey("dbaccess.endpoint.url",
            "Connection string to the database in which the user should be created");
    
    ConfigKey<String> PROTOCOL_SCHEME = ConfigKeys.newStringConfigKey("dbaccess.url.protocol.scheme",
            "Protocol to be used when constructing the database URL");

    ConfigKey<String> HOST = ConfigKeys.newStringConfigKey("dbaccess.host",
            "Host where the database is running");

    ConfigKey<String> PORT = ConfigKeys.newStringConfigKey("dbaccess.port",
            "Port for connecting to the database");

    ConfigKey<String> ADMIN_USER = ConfigKeys.newStringConfigKey("dbaccess.admin.user",
            "Admin user to be used when creating the user");

    ConfigKey<String> ADMIN_PASSWORD = ConfigKeys.newStringConfigKey("dbaccess.admin.password",
            "Admin password to be used when creating the user");

    ConfigKey<String> DATABASE = ConfigKeys.newStringConfigKey("dbaccess.database",
            "Database in which the user should be created");

    ConfigKey<AccessModes> ACCESS_MODE = ConfigKeys.newConfigKey(AccessModes.class, "dbaccess.access.mode",
            "What access should be granted: read-only, read-write, or custom; in RW mode permissions should be supplied,"
            + " and in custom a script should be supplied");

    public enum AccessModes { READ_ONLY, READ_WRITE, CUSTOM;
        @Override
        public String toString() {
            return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, super.toString());
        }
    }
    public enum Permission { INSERT, UPDATE, DELETE }
    
    ConfigKey<String> ACCESS_SCRIPT = ConfigKeys.newStringConfigKey("dbaccess.access.script",
            "Script that should be run to give access, when access mode is custom; optionally can access `${user}` and `${pass}` and `${db}`");

    @SuppressWarnings("serial")
    ConfigKey<List<Permission>> PERMISSIONS = ConfigKeys.newConfigKey(new TypeToken<List<Permission>>() { }, "dbaccess.permissions",
            "Permissions to grant to the new user, in addition to default SELECT in READ_WRITE mode, of the form `INSERT`, `UPDATE`, or `DELETE`.");

    BasicAttributeSensorAndConfigKey<String> USERNAME = new BasicAttributeSensorAndConfigKey<>(
            String.class, "dbaccess.username", "Displays the username which has been created");

    BasicAttributeSensorAndConfigKey<String> PASSWORD = new BasicAttributeSensorAndConfigKey<>(
            String.class, "dbaccess.password", "Displays the password which has been created");

    // "uri" is the most common
    // also "host, port, dbname, username, password"
    // as per https://docs.cloudfoundry.org/devguide/services/user-provided.html
    // and http://cloud.spring.io/spring-cloud-connectors/spring-cloud-cloud-foundry-connector.html
    AttributeSensor<String> CF_EXPORT_URI = Sensors.newStringSensor("uri");
    AttributeSensor<String> CF_EXPORT_HOST = Sensors.newStringSensor("host");
    AttributeSensor<String> CF_EXPORT_PORT = Sensors.newStringSensor("port");
    AttributeSensor<String> CF_EXPORT_USERNAME = Sensors.newStringSensor("username");
    AttributeSensor<String> CF_EXPORT_PASSWORD = Sensors.newStringSensor("password");
    AttributeSensor<String> CF_EXPORT_DBNAME = Sensors.newStringSensor("dbname");
    AttributeSensor<String> CF_EXPORT_JDBC_URL = Sensors.newStringSensor("jdbcUrl");

    // we could support this; although probably people would want a *per-bind* user which is trickier
//    ConfigKey<Boolean> DEFER_USER_CREATION_TO_BIND = ConfigKeys.newBooleanConfigKey("dbaccess.defer_user_creation_to_bind",
//        "Normally user creation is done on start, but it can be deferred to the first bind, and removed on last unbind");

    DatabaseAccessClient createClient();
}
