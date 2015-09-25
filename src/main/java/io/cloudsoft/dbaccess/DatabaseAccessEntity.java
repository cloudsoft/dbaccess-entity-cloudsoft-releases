package io.cloudsoft.dbaccess;

import io.cloudsoft.cfentity.CloudFoundryService;
import io.cloudsoft.dbaccess.client.DatabaseAccessClient;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.BasicAttributeSensorAndConfigKey;
import org.apache.brooklyn.entity.database.DatastoreMixins;
import org.apache.brooklyn.entity.stock.BasicApplication;

public interface DatabaseAccessEntity extends BasicApplication, DatastoreMixins.HasDatastoreUrl, CloudFoundryService {

    ConfigKey<String> ENDPOINT_URL = ConfigKeys.newStringConfigKey("dbaccess.endpoint.url",
            "Connection string to the database in which the user should be created");

    ConfigKey<String> ADMIN_USER = ConfigKeys.newStringConfigKey("dbaccess.admin.user",
            "Admin user to be used when creating the user");

    ConfigKey<String> ADMIN_PASSWORD = ConfigKeys.newStringConfigKey("dbaccess.admin.password",
            "Admin password to be used when creating the user");

    ConfigKey<String> DATABASE = ConfigKeys.newStringConfigKey("dbaccess.database",
            "Database in which the user should be created");

    BasicAttributeSensorAndConfigKey<String> USERNAME = new BasicAttributeSensorAndConfigKey<>(
            String.class, "dbaccess.username", "Displays the username which has been created");

    BasicAttributeSensorAndConfigKey<String> PASSWORD = new BasicAttributeSensorAndConfigKey<>(
            String.class, "dbaccess.password", "Displays the password which has been created");

    DatabaseAccessClient createClient();
}
