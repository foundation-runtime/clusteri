package com.cisco.oss.foundation.cluster.test;

import com.cisco.oss.foundation.cluster.registry.MasterSlaveListener;
import com.cisco.oss.foundation.cluster.registry.MasterSlaveRegistry;
import com.cisco.oss.foundation.configuration.CcpConstants;
import com.cisco.oss.foundation.environment.utils.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by Yair Ogen (yaogen) on 13/04/2016.
 */
public class Main2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main2.class);

    public static void main(String[] args) {
        System.setProperty(CcpConstants.RPM_SOFTWARE_NAME,"comp1");
        EnvUtils.updateEnv(CcpConstants.FQDN,"localhost2");
        EnvUtils.updateEnv("STACK_NAME","test");
        EnvUtils.updateEnv("STACK_VERSION","001");
        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test1", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST2] - I AM MASTER");
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST2] - I AM slave");
            }
        });

        try {
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
