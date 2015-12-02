package io.cloudsoft.dbaccess;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatabaseAccessEntityConfigAndSensorsTest extends BrooklynMgmtUnitTestSupport {

    @Test
    public void testConfigFromUrl() {
        MySqlDatabaseAccessEntity e = mgmt.getEntityManager().createEntity(
            EntitySpec.create(MySqlDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.ENDPOINT_URL, 
                    "mysql://user:pass@host:1234/dbname"));
        
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.PROTOCOL_SCHEME), "mysql" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.HOST), "host" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.PORT), "1234" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.ADMIN_USER), "user" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.ADMIN_PASSWORD), "pass" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.DATABASE), "dbname" );
    }
    
    @Test
    public void testSensorFromUrl() {
        MySqlDatabaseAccessEntity e = mgmt.getEntityManager().createEntity(
            EntitySpec.create(MySqlDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.ENDPOINT_URL, 
                    "mysql://user:pass@host:1234/dbname"));
        
        DatabaseAccessEntityImpl ee = ((DatabaseAccessEntityImpl)Entities.deproxy(e));
        
        ee.exportCfSensorsSet("bob", "p4ss", ee.createClient().getJdbcUrl("bob", "p4ss"));
        
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_HOST), "host" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_PORT), "1234" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_USERNAME), "bob" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_PASSWORD), "p4ss" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_DBNAME), "dbname" );
        
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_URI), 
            "mysql://bob:p4ss@host:1234/dbname" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.DATASTORE_URL), 
            "mysql://bob:p4ss@host:1234/dbname" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_JDBC_URL), 
            "jdbc:mysql://host:1234/dbname?user=bob&password=p4ss");
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.PORT), "1234" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.ADMIN_USER), "user" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.ADMIN_PASSWORD), "pass" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.DATABASE), "dbname" );
    }

    // generalization of above
    /** assumes input is for user:pass@host:1234 and created user is bob:p4ss */
    public void doTestDbType(EntitySpec<? extends DatabaseAccessEntity> spec, 
            String uri, String jdbcUrl) {
        DatabaseAccessEntity e = mgmt.getEntityManager().createEntity(spec);
        
        DatabaseAccessEntityImpl ee = ((DatabaseAccessEntityImpl)Entities.deproxy(e));
        
        ee.exportCfSensorsSet("bob", "p4ss", ee.createClient().getJdbcUrl("bob", "p4ss"));
        
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_HOST), "host" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_PORT), "1234" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_USERNAME), "bob" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_PASSWORD), "p4ss" );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_DBNAME), "dbname" );
        
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_URI), uri );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.DATASTORE_URL), uri );
        Assert.assertEquals( e.sensors().get(DatabaseAccessEntity.CF_EXPORT_JDBC_URL), jdbcUrl);
            
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.PORT), "1234" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.ADMIN_USER), "user" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.ADMIN_PASSWORD), "pass" );
        Assert.assertEquals( e.getConfig(DatabaseAccessEntity.DATABASE), "dbname" );
    }

    @Test
    public void testMysql() {
        doTestDbType(EntitySpec.create(MySqlDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.ENDPOINT_URL, 
                    "mysql://user:pass@host:1234/dbname"),
                    "mysql://bob:p4ss@host:1234/dbname",
                    
                    // https://www.petefreitag.com/articles/jdbc_urls/
                    // http://www.herongyang.com/JDBC/MySQL-JDBC-Driver-Connection-URL.html
                    "jdbc:mysql://host:1234/dbname?user=bob&password=p4ss");
    }

    @Test
    public void testOracle() {
        doTestDbType(EntitySpec.create(OracleDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.ENDPOINT_URL, 
                    "oracle://user:pass@host:1234/dbname"),
                    "oracle://bob:p4ss@host:1234/dbname",
                    
                    // http://stackoverflow.com/questions/1054105/url-string-format-for-connecting-to-oracle-database-with-jdbc
                    "jdbc:oracle:thin:bob/p4ss@host:1234:dbname");
    }

    @Test
    public void testPostgres() {
        doTestDbType(EntitySpec.create(PostgresDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.ENDPOINT_URL, 
                    "postgres://user:pass@host:1234/dbname"),
                    "postgres://bob:p4ss@host:1234/dbname",
                    
                    // https://www.petefreitag.com/articles/jdbc_urls/
                    "jdbc:postgres://host:1234/dbname?user=bob&password=p4ss");
    }

    @Test
    public void testMsSql() {
        doTestDbType(EntitySpec.create(MsSqlDatabaseAccessEntity.class)
                .configure(DatabaseAccessEntity.ENDPOINT_URL, 
                    "mssql://user:pass@host:1234/dbname"),
                    "mssql://bob:p4ss@host:1234/dbname",
                    
                    // https://www.petefreitag.com/articles/jdbc_urls/
                    // but https://msdn.microsoft.com/en-us/library/ms378428(v=sql.110).aspx
                    // says "jdbc:sqlserver"
                    "jdbc:microsoft:sqlserver://host:1234;databasename=dbname;user=bob;password=p4ss");
    }

}
