package io.cloudsoft.dbaccess;

import org.apache.brooklyn.api.entity.ImplementedBy;

@ImplementedBy(OracleDatabaseAccessEntityImpl.class)
public interface OracleDatabaseAccessEntity extends DatabaseAccessEntity{

}
