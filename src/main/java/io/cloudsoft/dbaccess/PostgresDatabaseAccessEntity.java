package io.cloudsoft.dbaccess;

import org.apache.brooklyn.api.entity.ImplementedBy;

@ImplementedBy(PostgresDatabaseAccessEntityImpl.class)
public interface PostgresDatabaseAccessEntity extends DatabaseAccessEntity {

}
