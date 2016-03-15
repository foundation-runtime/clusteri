package com.cisco.oss.foundation.cluster.masterslave;

/**
 * Created by Yair Ogen (yaogen) on 14/02/2016.
 */
public interface MastershipElector {

    void init(String id, String jobName);
    boolean isReady();
    boolean isActiveVersion(String currentVersion);
    boolean isActiveDataCenter(String currentDataCenter);
    boolean isMaster();
    void close();
    String getActiveVersion();
}
