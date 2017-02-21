package io.cloudsoft.dbaccess;

import java.net.InetAddress;
import java.net.URL;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.entity.database.DatastoreMixins;
import org.apache.brooklyn.entity.database.mysql.MySqlNode;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

import io.cloudsoft.dbaccess.DatabaseAccessEntity.AccessModes;
import io.cloudsoft.dbaccess.DatabaseAccessEntity.Permissions;
import io.cloudsoft.dbaccess.client.MySqlAccessClient;

public class MySqlDatabaseAccessEntityIntegrationTest extends AbstractDatabaseAccessEntityIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlDatabaseAccessEntityIntegrationTest.class);
    
    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        URL url = Resources.getResource("mysql-creation.sql");
        String creationTemplate = Resources.toString(url, Charsets.UTF_8);

        String creationScript = TemplateProcessor.processTemplateContents(creationTemplate, ImmutableMap.of(
                "database", TEST_DATABASE,
                "user", getAdminUserName(),
                "password", TEST_ADMIN_PASSWORD,
                "hostname", InetAddress.getLocalHost().getHostName()
        ));

        databaseNode = app.createAndManageChild(EntitySpec.create(MySqlNode.class)
            .configure(DatastoreMixins.CREATION_SCRIPT_CONTENTS, creationScript));
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test(groups="Integration")
    public void testNoPasswordProvided() throws Exception {
        EntitySpec<MySqlDatabaseAccessEntity> spec = EntitySpec.create(MySqlDatabaseAccessEntity.class);
        MySqlDatabaseAccessEntity entity = createDatabaseAccessEntity(spec);
        runTest(entity);
    }

    @Test(groups="Integration")
    public void testWithPasswordProvided() throws Exception {
        EntitySpec<MySqlDatabaseAccessEntity> spec = EntitySpec.create(MySqlDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.USERNAME, TEST_USERNAME)
                .configure(DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
        MySqlDatabaseAccessEntity entity = createDatabaseAccessEntity(spec);
        EntityAsserts.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.USERNAME, TEST_USERNAME);
        EntityAsserts.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
        runTest(entity);
    }

    @Override
    protected String getExpectedAccessDeniedMessage(String username) {
        return String.format("Access denied for user '%s'@", username);
    }

    @Override
    protected String getDatabaseNamesStatement() {
        return String.format("SELECT dbname FROM testtable WHERE dbname = '%s';", TEST_DATABASE);
    }

    @Override
    protected String getDriverName() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    protected String getAdminUserName() {
        return "mysqladmin";
    }

    // expects a local MySQL on 3306
    @Test(groups="Live")
    public void testLocalDatabase() {
        String script = Strings.lines(
            "/* This is a comment */",
            "-- another comment",
            "CREATE TABLE /* ignored */ ${db}_${user} (id INT NOT NULL);",
            "GRANT SELECT ON ${db}.${db}_${user} TO '${user}'@'localhost';",
            "GRANT SELECT ON ${db}.${db}_${user} TO '${user}'@'%';");

        MySqlAccessClient access = new MySqlAccessClient("mysql", "127.0.0.1", "3306", "root", "123456", "customers", 
            AccessModes.CUSTOM, script, MutableList.<Permissions>of());
        try {
            access.createUser("sample_test_user", "pwd");
        } catch (Exception e) {
            LOG.warn("Deleting user specially after failure", e);
            access.deleteUser("sample_test_user");
            throw e;
        }
        access.deleteUser("sample_test_user");
        access.execute("DROP TABLE ${db}_${user}", "sample_test_user", null);
    }

    public static void main(String[] args) {
        LOG.info("Running simple local MySQL test");
        new MySqlDatabaseAccessEntityIntegrationTest().testLocalDatabase();
    }
    
}
