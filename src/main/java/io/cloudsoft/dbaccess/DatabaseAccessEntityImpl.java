package io.cloudsoft.dbaccess;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;

import brooklyn.entity.basic.BasicEntityImpl;
import brooklyn.util.text.Identifiers;
import io.cloudsoft.dbaccess.client.DatabaseAccessClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DatabaseAccessEntityImpl extends BasicEntityImpl implements DatabaseAccessEntity {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAccessEntityImpl.class);

    @Override
    public void init() {
        super.init();
        String endpoint = getConfig(ENDPOINT_URL);
        Preconditions.checkNotNull(endpoint, "endpoint URL must be set");
        String database = config().get(DATABASE);
        Preconditions.checkNotNull(database, "database must be set");
        Preconditions.checkNotNull(config().get(ADMIN_USER), "admin user must be set");
        Preconditions.checkNotNull(config().get(ADMIN_PASSWORD), "admin password must be set");
        String username = config().get(USERNAME);
        String password = config().get(PASSWORD);
        if (username == null) {
            username = ("user_" + Identifiers.makeRandomJavaId(6)).toLowerCase();
        }
        if (password == null) {
            password = Identifiers.makeRandomJavaId(12);
        }
        setAttribute(USERNAME, username);
        setAttribute(PASSWORD, password);
        LOG.info("Creating user");
        DatabaseAccessClient client = createClient();
        client.createUser(username, password);
        setAttribute(DATASTORE_URL, String.format("%s%s?user=%s&password=%s", endpoint, database, username, password));
    }

}
