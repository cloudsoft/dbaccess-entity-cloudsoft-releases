package io.cloudsoft.dbaccess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.entity.database.DatastoreMixins;
import org.apache.brooklyn.entity.database.postgresql.PostgreSqlNode;
import org.apache.brooklyn.test.EntityTestUtils;
import org.python.netty.channel.DefaultAddressedEnvelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class PostgresDatabaseAccessEntityIntegrationTest extends BrooklynAppLiveTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresDatabaseAccessEntityIntegrationTest.class);
    private static final String TEST_DATABASE = "testdatabase";
    private static final String TEST_USERNAME = "testusername";
    private static final String TEST_PASSWORD = "testpassword";
    private static final String TEST_ADMIN_USER = "postgres";
    private static final String TEST_ADMIN_PASSWORD = "testadminpassword";
    private Connection connect = null;
    private Statement statement = null;

    protected PostgreSqlNode postgreSqlNode;

    protected Location testLocation;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testLocation = app.newLocalhostProvisioningLocation();
        postgreSqlNode = app.createAndManageChild(EntitySpec.create(PostgreSqlNode.class)
                .configure(PostgreSqlNode.INITIALIZE_DB, true)
                .configure(PostgreSqlNode.DATABASE, TEST_DATABASE));
    }


    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test()
    public void testNoPasswordProvided() throws Exception {
        EntitySpec<PostgresDatabaseAccessEntity> spec = EntitySpec.create(PostgresDatabaseAccessEntity.class);
        testAccess(createPostgresDatabaseAccessEntity(spec));
    }

    @Test()
    public void testWithPasswordProvided() throws Exception  {
        EntitySpec<PostgresDatabaseAccessEntity> spec = EntitySpec.create(PostgresDatabaseAccessEntity.class)
                .configure(PostgresDatabaseAccessEntity.USERNAME, TEST_USERNAME)
                .configure(PostgresDatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
        PostgresDatabaseAccessEntity entity = createPostgresDatabaseAccessEntity(spec);
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.USERNAME, TEST_USERNAME);
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
        testAccess(createPostgresDatabaseAccessEntity(spec));
    }
    
    protected PostgresDatabaseAccessEntity createPostgresDatabaseAccessEntity(EntitySpec<PostgresDatabaseAccessEntity> spec){
        app.start(ImmutableList.of(testLocation));
        spec.configure(PostgresDatabaseAccessEntity.DATABASE, TEST_DATABASE);
        spec.configure(DatabaseAccessEntity.ADMIN_USER, TEST_ADMIN_USER);
        spec.configure(DatabaseAccessEntity.ADMIN_PASSWORD, TEST_ADMIN_PASSWORD);
        spec.configure(DatabaseAccessEntity.ENDPOINT_URL, DependentConfiguration.attributeWhenReady(postgreSqlNode, DatastoreMixins.DATASTORE_URL));
        return app.createAndManageChild(spec);
    }

    protected void testAccess(PostgresDatabaseAccessEntity entity) throws Exception {
        String database = entity.config().get(DatabaseAccessEntity.DATABASE);
        String username = entity.sensors().get(DatabaseAccessEntity.USERNAME);
        String password = entity.sensors().get(DatabaseAccessEntity.PASSWORD);
        String url = entity.sensors().get(DatabaseAccessEntity.DATASTORE_URL);
        String endpoint = entity.config().get(DatabaseAccessEntity.ENDPOINT_URL);
        Assert.assertEquals(url, String.format("%s%s?user=%s&password=%s", endpoint, database, username, password));
        connect(entity.getAttribute(DatastoreMixins.DATASTORE_URL));
        ResultSet results = statement.executeQuery("SELECT datname FROM pg_database WHERE datname = 'db';");
        results.last();
        Assert.assertEquals(results.getRow(), 1);
        String dbName = results.getString(0);
        Assert.assertEquals(dbName, "db");
    }

    public void connect(String url) throws Exception {
        try {
            Class.forName("org.postgresql.Driver");
            String jdbcUrl = String.format("jdbc:%s", url);
            LOG.info("Connecting to " + jdbcUrl);

            connect = DriverManager.getConnection(jdbcUrl);

            // Statements allow to issue SQL queries to the database
            statement = connect.createStatement();

        } catch (Exception ex) {
            close();
            throw ex;
        }
    }

    public void close() throws Exception {
        if (statement != null) {
            statement.close();
            statement = null;
        }

        if (connect != null) {
            connect.close();
            connect = null;
        }
    }
}
