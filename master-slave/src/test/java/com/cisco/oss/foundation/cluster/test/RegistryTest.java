package com.cisco.oss.foundation.cluster.test;

import com.cisco.oss.foundation.cluster.mongo.MongoClient;
import com.cisco.oss.foundation.cluster.registry.MasterSlaveListener;
import com.cisco.oss.foundation.cluster.registry.MasterSlaveRegistry;
import com.cisco.oss.foundation.configuration.CcpConstants;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Yair Ogen (yaogen) on 21/01/2016.
 */
public class RegistryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryTest.class);

    @Test
    public void runOnelistener(){


        System.setProperty(CcpConstants.RPM_SOFTWARE_NAME,"dummy_component");
//        MongoClient instance = MongoClient.INSTANCE;

        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test1", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST1] - I AM MASTER");
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST1] - I AM slave");
            }
        });
    }
}
