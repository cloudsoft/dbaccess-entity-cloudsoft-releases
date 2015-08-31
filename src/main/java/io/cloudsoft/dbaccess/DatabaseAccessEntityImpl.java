package io.cloudsoft.dbaccess;

import org.apache.brooklyn.entity.stock.BasicEntityImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DatabaseAccessEntityImpl extends BasicEntityImpl implements DatabaseAccessEntity {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseAccessEntityImpl.class);

    @Override
    public void init() {
        super.init();
        LOG.info("Creating user");
    }

}
