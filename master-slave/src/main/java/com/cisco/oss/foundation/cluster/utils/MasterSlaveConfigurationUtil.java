package com.cisco.oss.foundation.cluster.utils;

import com.cisco.oss.foundation.configuration.CcpConstants;
import org.apache.commons.configuration.Configuration;

import com.cisco.oss.foundation.configuration.ConfigurationFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MasterSlaveConfigurationUtil {

    public static final String INSTANCE_ID = getUniqueProcessName();
    public static final String COMPONENT_NAME = getComponentName();

    public static final String ACTIVE_DATA_CENTER = System.getenv("DATA_CENTER");

    private static Configuration configuration = ConfigurationFactory.getConfiguration();

    private static Pattern MONGO_SERVERS = Pattern.compile("mongodb\\.([0-9]+)\\.host");


    public static List<Pair<String, Integer>> getMongodbServers() {
        List<Pair<String, Integer>> servers = new ArrayList<>();
        Iterator<String> keys = configuration.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            Matcher matcher = MONGO_SERVERS.matcher(key);
            if (matcher.matches()) {
                String index = matcher.group(1);
                String host = configuration.getString(key);
                Integer port = configuration.getInt("mongodb." + index + ".port");
                servers.add(Pair.of(host,port));
            }
        }
        return servers;
    }


    public static String getMongodbName() {
        return configuration.getString("mongodb.db.name", "cluster-db");
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
        if (installDir != null) {
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
