package com.cisco.oss.foundation.cluster.masterslave.consul;

import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.cisco.oss.foundation.configuration.CcpConstants;

/**
 * Created by Yair Ogen (yaogen) on 15/03/2016.
 */
public class OpenstackConsulMastershipElector extends ConsulMastershipElector {

    private static final String STACK_NAME = "STACK_NAME";
    private static final String STACK_VERSION = "STACK_VERSION";
    private String activeVersionKey = "/activated/" + System.getenv(STACK_NAME);
    private String activeVersion = System.getenv(STACK_VERSION);

    @Override
    protected String getActiveVersionKey() {
        return activeVersionKey;
    }

    @Override
    public String getActiveVersion() {
        return activeVersion;
    }
}
