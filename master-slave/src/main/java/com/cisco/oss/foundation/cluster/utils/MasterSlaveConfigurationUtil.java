package com.cisco.oss.foundation.cluster.utils;

import com.cisco.oss.foundation.cluster.mongo.MissingMongoConfigException;
import com.cisco.oss.foundation.configuration.CcpConstants;
import org.apache.commons.configuration.Configuration;

import com.cisco.oss.foundation.configuration.ConfigurationFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper method for reading properties from the config object.
 * Created by Yair Ogen (yaogen) on 14/01/2016.
 */
public class MasterSlaveConfigurationUtil {

    public static final String INSTANCE_ID = getUniqueProcessName();
    public static final String COMPONENT_NAME = getComponentName();

    public static final String ACTIVE_DATA_CENTER = System.getenv("DATA_CENTER");

    private static Configuration configuration = ConfigurationFactory.getConfiguration();

    private static Pattern MONGO_SERVERS = Pattern.compile("mongodb\\.([0-9]+)\\.host");


    /**
     * read pairs of host and port from configuration.
     * @return List of Pairs. Each pair contains host and port.
     */
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

        if(servers.isEmpty()){
            throw new MissingMongoConfigException("missing mongo db configuration. you have to define at least one array memeber for 'mongodb.<index>.host' and 'mongodb.<index>.port'");
        }

        return servers;
    }

    /**
     * @return the mongo db name
     */
    public static String getMongodbName() {
        return configuration.getString("mongodb.master-slave-db.name", "master-slave-clusters");
    }

    /**
     * @param name the logical name used in the registry class
     * @return the lease time in seconds
     */
    public static int getMasterSlaveLeaseTime(String name) {
        return configuration.getInt(name + ".masterSlave.leaseTime", 30);
    }

    public static MasterSlaveMultiplicity getMasterSlaveMultiplicity(String name) {
        String multiplicity = configuration.getString(name + ".masterSlave.mastership.multiplicity", "single");
        return MasterSlaveMultiplicity.newInstance(multiplicity);
    }

    public static boolean isSingleAcrossMDC(String name){
        return configuration.getBoolean(name + ".masterSlave.mastership.singleAcrossMDC", true);
    }

    public static boolean isSingleAcrossVersion(String name){
        return configuration.getBoolean(name + ".masterSlave.mastership.singleAcrossVersion", true);
    }

    /**
     * @return the component unique name. see wiki for more info: https://github.com/foundation-runtime/cluster/wiki/Master-Slave#instance-identification
     */
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

    /**
     * @return the component name. this is not unique and is shared by all instances of the component. see reference: https://github.com/foundation-runtime/cluster/wiki/Master-Slave#instance-identification
     */
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
