package com.cisco.oss.foundation.cluster.utils;

import com.cisco.oss.foundation.cluster.mongo.MissingMongoConfigException;
import com.cisco.oss.foundation.configuration.CcpConstants;
import org.apache.commons.configuration.Configuration;

import com.cisco.oss.foundation.configuration.ConfigurationFactory;
import org.apache.commons.lang3.StringUtils;
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

    private static String mongodbServerConfigPrefix = "mongodb";

    private static Pattern MONGO_SERVERS = Pattern.compile(mongodbServerConfigPrefix + "\\.([0-9]+)\\.host");

    private final static List<Pair<String, Integer>> mongodbServers = new ArrayList<>();

    private static String mongoUserName = "";
    private static String mongoPassword = "";
    private static boolean isMongoAuthenticationEnabled = false;
    private static boolean isMongoAutoStart = true;

    /**
     * When set to read mongo servers from configuration you can override the configuration prefix using this method.
     * If called with emppty or null prefix, the default will be used
     *
     * @param mongodbServerConfigPrefix - the new prefix. default is: 'mongodb'.
     */
    public static void setMongodbServerConfigPrefix(String mongodbServerConfigPrefix) {
        if (StringUtils.isNotBlank(mongodbServerConfigPrefix)) {
            MasterSlaveConfigurationUtil.mongodbServerConfigPrefix = mongodbServerConfigPrefix;
            MONGO_SERVERS = Pattern.compile(MasterSlaveConfigurationUtil.mongodbServerConfigPrefix + "\\.([0-9]+)\\.host");
        }
    }


    /**
     * Use this API if you want to external set the mongo servers list and prevent the lib from fetching the info from configuration
     *
     * @param mongodbServers
     */
    public static void setMongodbServers(List<Pair<String, Integer>> mongodbServers) {
        MasterSlaveConfigurationUtil.mongodbServers.addAll(mongodbServers);
    }


    /**
     * read pairs of host and port from configuration.
     *
     * @return List of Pairs. Each pair contains host and port.
     */
    public static List<Pair<String, Integer>> getMongodbServers() {

        if (mongodbServers.isEmpty()) {
            Iterator<String> keys = configuration.getKeys();
            while (keys.hasNext()) {
                String key = keys.next();
                Matcher matcher = MONGO_SERVERS.matcher(key);
                if (matcher.matches()) {
                    String index = matcher.group(1);
                    String host = configuration.getString(key);
                    Integer port = configuration.getInt(mongodbServerConfigPrefix + "." + index + ".port");
                    mongodbServers.add(Pair.of(host, port));
                }
            }
        }

        if (mongodbServers.isEmpty()) {
            throw new MissingMongoConfigException("missing mongo db configuration. you have to define at least one array memeber for 'mongodb.<index>.host' and 'mongodb.<index>.port'");
        }

        return mongodbServers;
    }

    /**
     * call this method first if you want to enable authenticated mongo db access
     *
     * @param user     the db user
     * @param password teh db pasword
     */
    public static void enableAuthentication(String user, String password) {

        if (StringUtils.isBlank(user)) {
            throw new IllegalArgumentException("mongo user can't be null or empty");
        }

        if (StringUtils.isBlank(password)) {
            throw new IllegalArgumentException("mongo password can't be null or empty");
        }

        isMongoAuthenticationEnabled = true;
        mongoUserName = user;
        mongoPassword = password;
    }

    public static boolean isMongoAuthenticationEnabled() {
        return isMongoAuthenticationEnabled;
    }

    public static void setIsMongoAutoStart(boolean isMongoAutoStart){
        MasterSlaveConfigurationUtil.isMongoAutoStart = isMongoAutoStart;
    }

    public static boolean isMongoAutoStart() {
        return isMongoAutoStart;
    }

    public static Pair<String, String> getMongoUserCredentials() {
        return Pair.of(mongoUserName, mongoPassword);
    }

    public static String getMasterSlaveImpl() {
        return configuration.getString("masterSlave.impl", "consul");
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

//    public static HostAndPort getConsulHostAndPort(String name) {
//        String consultHostAndPort = configuration.getString("consul.hostAndPort", "localhost:8500");
//        HostAndPort hostAndPort = HostAndPort.fromString(consultHostAndPort);
//        return hostAndPort;
//    }

    public static boolean isSingleAcrossMDC(String name) {
        return configuration.getBoolean(name + ".masterSlave.mastership.singleAcrossMDC", true);
    }

    public static boolean isSingleAcrossVersion(String name) {
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
        String applicationName = System.getenv(CcpConstants.RPM_SOFTWARE_NAME);

        if (applicationName == null) {
            applicationName = System.getenv(CcpConstants.ARTIFACT_NAME);
        }

        if (applicationName == null) {
            applicationName = System.getProperty(CcpConstants.RPM_SOFTWARE_NAME);
        }

        if (applicationName == null) {
            applicationName = System.getProperty(CcpConstants.RPM_SOFTWARE_NAME);
        }

        if (applicationName == null) {
            throw new IllegalArgumentException(CcpConstants.RPM_SOFTWARE_NAME + " environment variable is mandatory when CCP is enabled");
        }
        return applicationName;
    }


}
