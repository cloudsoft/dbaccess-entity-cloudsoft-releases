package io.cloudsoft.dbaccess;


import brooklyn.entity.proxying.ImplementedBy;

@ImplementedBy(PostgresDatabaseAccessEntityImpl.class)
public interface PostgresDatabaseAccessEntity extends DatabaseAccessEntity {

}
