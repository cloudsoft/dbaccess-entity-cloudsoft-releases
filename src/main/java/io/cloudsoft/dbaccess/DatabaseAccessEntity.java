package io.cloudsoft.dbaccess;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.BasicAttributeSensorAndConfigKey;
import org.apache.brooklyn.entity.stock.BasicEntity;

import io.cloudsoft.dbaccess.client.DBAccessClient;

public interface DatabaseAccessEntity extends BasicEntity {

    ConfigKey<String> ENDPOINT_URL = ConfigKeys.newStringConfigKey("dbaccess.enpoint.url",
            "Connection string to the database in which the user should be created");

    ConfigKey<String> ADMIN_PASSWORD = ConfigKeys.newStringConfigKey("dbaccess.admin.password",
            "Connection string to the database in which the user should be created");

    ConfigKey<String> DATABASE = ConfigKeys.newStringConfigKey("dbaccess.database",
            "Database in which the user should be created");

    BasicAttributeSensorAndConfigKey<String> USERNAME = new BasicAttributeSensorAndConfigKey<>(
            String.class, "dbaccess.username", "Displays the username which has been created");

    BasicAttributeSensorAndConfigKey<String> PASSWORD = new BasicAttributeSensorAndConfigKey<>(
            String.class, "dbaccess.password", "Displays the password which has been created");

    DBAccessClient createClient();
}
