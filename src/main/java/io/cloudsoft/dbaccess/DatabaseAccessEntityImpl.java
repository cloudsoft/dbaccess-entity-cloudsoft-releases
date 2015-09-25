package io.cloudsoft.dbaccess;

import org.apache.brooklyn.entity.stock.BasicApplicationImpl;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import io.cloudsoft.dbaccess.client.DatabaseAccessClient;

import com.google.common.base.Preconditions;

public abstract class DatabaseAccessEntityImpl extends BasicApplicationImpl implements DatabaseAccessEntity {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAccessEntityImpl.class);
    private final AtomicBoolean userDeleted = new AtomicBoolean(false);

    @Override
    public void init() {
        super.init(); 
        Preconditions.checkNotNull(config().get(ENDPOINT_URL), "endpoint URL must be set");
        Preconditions.checkNotNull(config().get(DATABASE), "database must be set");
        Preconditions.checkNotNull(config().get(ADMIN_USER), "admin user must be set");
        Preconditions.checkNotNull(config().get(ADMIN_PASSWORD), "admin password must be set");
    }

    protected String makeDatastoreUrl(String endpoint, String database, String username, String password) {
        return String.format("%s%s?user=%s&password=%s", endpoint, database, username, password);
    }

    @Override
    public void stop() {
        unbind();
        super.stop();
    }
    
    @Override
    public void bind() {
        String endpoint = config().get(ENDPOINT_URL);
        String database = config().get(DATABASE);
    	String username = config().get(USERNAME);
        String password = config().get(PASSWORD);
        if (username == null) {
            username = ("user_" + Identifiers.makeRandomJavaId(6)).toLowerCase();
        }
        if (password == null) {
            password = Identifiers.makeRandomJavaId(12);
        }
        sensors().set(USERNAME, username);
        sensors().set(PASSWORD, password);
        config().set(USERNAME, username);
        config().set(PASSWORD, password);
        this.setDisplayName(String.format("DBAccess (%s): %s", database, username));
        LOG.info("Creating user");
        DatabaseAccessClient client = createClient();
        client.createUser(username, password);
        sensors().set(DATASTORE_URL, makeDatastoreUrl(endpoint, database, username, password));
    }
    
    @Override
    public void unbind() {
    	if (!userDeleted.getAndSet(true)) {
            String username = getAttribute(USERNAME);
            DatabaseAccessClient client = createClient();
            client.deleteUser(username);
        }
    }
}
