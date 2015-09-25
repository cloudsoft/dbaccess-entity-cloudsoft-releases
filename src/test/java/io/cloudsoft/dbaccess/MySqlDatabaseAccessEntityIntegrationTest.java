package io.cloudsoft.dbaccess;

import java.net.InetAddress;
import java.net.URL;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.entity.database.DatastoreMixins;
import org.apache.brooklyn.entity.database.mysql.MySqlNode;
import org.apache.brooklyn.test.EntityTestUtils;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

public class MySqlDatabaseAccessEntityIntegrationTest extends AbstractDatabaseAccessEntityIntegrationTest {

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

    @Test()
    public void testNoPasswordProvided() throws Exception {
        EntitySpec<MySqlDatabaseAccessEntity> spec = EntitySpec.create(MySqlDatabaseAccessEntity.class);
        MySqlDatabaseAccessEntity entity = createDatabaseAccessEntity(spec);
        runTest(entity);
    }

    @Test()
    public void testWithPasswordProvided() throws Exception {
        EntitySpec<MySqlDatabaseAccessEntity> spec = EntitySpec.create(MySqlDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.USERNAME, TEST_USERNAME)
                .configure(DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
        MySqlDatabaseAccessEntity entity = createDatabaseAccessEntity(spec);
        entity.bind();
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.USERNAME, TEST_USERNAME);
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
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

}
