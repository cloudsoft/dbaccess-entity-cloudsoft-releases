package io.cloudsoft.dbaccess;

import org.apache.brooklyn.api.entity.ImplementedBy;

@ImplementedBy(MsSqlDatabaseAccessEntityImpl.class)
public interface MsSqlDatabaseAccessEntity extends DatabaseAccessEntity {
    
}
