package io.cloudsoft.dbaccess;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.basic.BasicApplicationImpl;
import brooklyn.util.text.Identifiers;
import io.cloudsoft.dbaccess.client.DatabaseAccessClient;

import com.google.common.base.Preconditions;

public abstract class DatabaseAccessEntityImpl extends BasicApplicationImpl implements DatabaseAccessEntity {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAccessEntityImpl.class);
    private final AtomicBoolean userDeleted = new AtomicBoolean(false);

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
        config().set(USERNAME, username);
        setAttribute(USERNAME, username);
        config().set(PASSWORD, password);
        setAttribute(PASSWORD, password);
        this.setDisplayName(String.format("DBAccess (%s): %s", database, username));
        LOG.info("Creating user");
        DatabaseAccessClient client = createClient();
        client.createUser(username, password);
        setAttribute(DATASTORE_URL, String.format("%s%s?user=%s&password=%s", endpoint, database, username, password));
    }

    @Override
    public void stop() {
        if (!userDeleted.getAndSet(true)) {
            String username = getAttribute(USERNAME);
            DatabaseAccessClient client = createClient();
            client.deleteUser(username);
        }
        super.stop();
    }
}
