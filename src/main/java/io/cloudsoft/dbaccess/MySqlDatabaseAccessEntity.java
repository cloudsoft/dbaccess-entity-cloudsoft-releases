package io.cloudsoft.dbaccess;


import brooklyn.entity.proxying.ImplementedBy;

@ImplementedBy(MySqlDatabaseAccessEntityImpl.class)
public interface MySqlDatabaseAccessEntity extends DatabaseAccessEntity {

}
