package com.cisco.oss.foundation.cluster.registry;

/**
 * Implement this listener for each logical unit of work you want to run in master and not run in slave
 * Created by Yair Ogen (yaogen) on 14/01/2016.
 */
public interface MasterSlaveListener {

    /**
     * callback method to indicate this running instance is now master
     */
    void goMaster();

    /**
     * callback method to indicate this running instance is now slave
     */
    void goSlave();

}
