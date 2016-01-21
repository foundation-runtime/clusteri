package com.cisco.oss.foundation.cluster.registry;

/**
 * Created by Yair Ogen (yaogen) on 14/01/2016.
 */
public interface MasterSlaveListener {

    void goMaster();
    void goSlave();

}
