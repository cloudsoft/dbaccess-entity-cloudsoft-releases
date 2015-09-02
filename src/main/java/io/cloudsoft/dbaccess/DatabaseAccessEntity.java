package io.cloudsoft.dbaccess;


import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.BasicApplication;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.database.DatastoreMixins;
import brooklyn.event.basic.BasicAttributeSensorAndConfigKey;
import io.cloudsoft.dbaccess.client.DatabaseAccessClient;

public interface DatabaseAccessEntity extends BasicApplication, DatastoreMixins.HasDatastoreUrl {

    ConfigKey<String> ENDPOINT_URL = ConfigKeys.newStringConfigKey("dbaccess.enpoint.url",
            "Connection string to the database in which the user should be created");

    ConfigKey<String> ADMIN_USER = ConfigKeys.newStringConfigKey("dbaccess.admin.user",
            "Admin user to be used when creating the user");

    ConfigKey<String> ADMIN_PASSWORD = ConfigKeys.newStringConfigKey("dbaccess.admin.password",
            "Admin password to be used when creating the user");

    ConfigKey<String> DATABASE = ConfigKeys.newStringConfigKey("dbaccess.database",
            "Database in which the user should be created");

    BasicAttributeSensorAndConfigKey<String> USERNAME = new BasicAttributeSensorAndConfigKey<String>(
            String.class, "dbaccess.username", "Displays the username which has been created");

    BasicAttributeSensorAndConfigKey<String> PASSWORD = new BasicAttributeSensorAndConfigKey<String>(
            String.class, "dbaccess.password", "Displays the password which has been created");

    DatabaseAccessClient createClient();
}
