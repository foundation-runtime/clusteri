package com.cisco.oss.foundation.cluster.utils;

import com.cisco.oss.foundation.configuration.CcpConstants;
import org.apache.commons.configuration.Configuration;

import com.cisco.oss.foundation.configuration.ConfigurationFactory;

public class MasterSlaveConfigurationUtil {

    public static final String INSTANCE_ID = getUniqueProcessName();
    public static final String COMPONENT_NAME = getComponentName();

    public static final String ACTIVE_DATA_CENTER = System.getenv("DATA_CENTER");

    private static Configuration configuration = ConfigurationFactory.getConfiguration();


    public static String getMongodbHost() {
        return configuration.getString("service.mongo.1.host");
    }

    public static int getMongodbPort() {
        return configuration.getInt("service.mongo.1.port");
    }

    public static String getMongodbName() {
        return configuration.getString("service.mongo.db.name", "cluster-db");
    }

    public static int getMasterSlaveLeaseTime(String name) {
        return configuration.getInt(name + ".masterSlave.leaseTime", 30);
    }

    public static String getUniqueProcessName() {

        String rpmSoftwareName = getComponentName();

        StringBuilder uniqueProcName = new StringBuilder();
        uniqueProcName.append(System.getenv(CcpConstants.FQDN));
        uniqueProcName.append("-").append(rpmSoftwareName);
        uniqueProcName.append("-").append(System.getenv(CcpConstants.ARTIFACT_VERSION));
        String installDir = System.getenv(CcpConstants.INSTALL_DIR);
        if(installDir != null){
            uniqueProcName.append("-").append(System.getenv(CcpConstants.INSTALL_DIR).replaceAll("/", "_"));
        }

        return uniqueProcName.toString();

    }

    public static String getComponentName() {
        String rpmSoftwareName = System.getenv(CcpConstants.RPM_SOFTWARE_NAME);

        if (rpmSoftwareName == null) {
            rpmSoftwareName = System.getenv(CcpConstants.ARTIFACT_NAME);
        }

        if (rpmSoftwareName == null) {
            rpmSoftwareName = System.getProperty(CcpConstants.RPM_SOFTWARE_NAME);
        }

        if (rpmSoftwareName == null) {
            throw new IllegalArgumentException(CcpConstants.RPM_SOFTWARE_NAME + " environment variable is mandatory when CCP is enabled");
        }
        return rpmSoftwareName;
    }


}
