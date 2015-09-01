package io.cloudsoft.dbaccess;

import org.apache.brooklyn.api.entity.ImplementedBy;

@ImplementedBy(MySqlDatabaseAccessEntityImpl.class)
public interface MySqlDatabaseAccessEntity extends DatabaseAccessEntity {

}
