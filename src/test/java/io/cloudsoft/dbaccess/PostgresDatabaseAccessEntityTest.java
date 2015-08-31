package io.cloudsoft.dbaccess;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.test.EntityTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class PostgresDatabaseAccessEntityTest extends BrooklynAppLiveTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresDatabaseAccessEntityTest.class);
    private static final String TEST_USERNAME = "testusername";
    private static final String TEST_PASSWORD = "testpassword";

    protected Location testLocation;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testLocation = app.newLocalhostProvisioningLocation();
    }


    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test()
    public void testNoPasswordProvided() {
        EntitySpec<PostgresDatabaseAccessEntity> spec = EntitySpec.create(PostgresDatabaseAccessEntity.class);
        PostgresDatabaseAccessEntity entity = app.createAndManageChild(spec);
        app.start(ImmutableList.of(testLocation));
        EntityTestUtils.assertAttributeEventuallyNonNull(entity, DatabaseAccessEntity.USERNAME);
        EntityTestUtils.assertAttributeEventuallyNonNull(entity, DatabaseAccessEntity.PASSWORD);
    }

    @Test()
    public void testWithPasswordProvided() {
        EntitySpec<PostgresDatabaseAccessEntity> spec = EntitySpec.create(PostgresDatabaseAccessEntity.class);
        PostgresDatabaseAccessEntity entity = app.createAndManageChild(spec);
        app.start(ImmutableList.of(testLocation));
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.USERNAME, TEST_USERNAME);
        EntityTestUtils.assertAttributeEqualsEventually(entity, DatabaseAccessEntity.PASSWORD, TEST_PASSWORD);
    }

}
