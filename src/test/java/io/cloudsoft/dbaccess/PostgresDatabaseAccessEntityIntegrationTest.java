package io.cloudsoft.dbaccess;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import brooklyn.entity.database.postgresql.PostgreSqlNode;
import brooklyn.entity.proxying.EntitySpec;
import brooklyn.test.EntityTestUtils;

public class PostgresDatabaseAccessEntityIntegrationTest extends AbstractDatabaseAccessEntityIntegrationTest {


    protected static final String TEST_ADMIN_USER = "postgres";

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        databaseNode = app.createAndManageChild(EntitySpec.create(PostgreSqlNode.class)
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
        testAccess(createDatabaseAccessEntity(spec));
    }

    @Test()
    public void testWithPasswordProvided() throws Exception  {
        EntitySpec<PostgresDatabaseAccessEntity> spec = EntitySpec.create(PostgresDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.USERNAME, TEST_USERNAME)
                .configure(DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
        PostgresDatabaseAccessEntity entity = createDatabaseAccessEntity(spec);
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.USERNAME, TEST_USERNAME);
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
        testAccess(entity);
    }

    @Override
    protected String getDatabaseNamesStatement() {
        return String.format("SELECT datname FROM pg_database WHERE datname = '%s';", TEST_DATABASE);
    }

    @Override
    public String getDriverName() {
        return "org.postgresql.Driver";
    }

    @Override
    protected String getAdminUserName() {
        return TEST_ADMIN_USER;
    }
}
