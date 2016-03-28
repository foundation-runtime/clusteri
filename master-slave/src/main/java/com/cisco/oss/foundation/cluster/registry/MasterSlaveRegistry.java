package com.cisco.oss.foundation.cluster.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Register your #MasterSlaveListener instances with this class.
 * It is an enum and you access its method via the #INSTANCE member
 * Created by Yair Ogen (yaogen) on 14/01/2016.
 */
public enum MasterSlaveRegistry {

    INSTANCE;

    private static Logger LOGGER = LoggerFactory.getLogger(MasterSlaveRegistry.class);

    private ConcurrentMap<String, MasterSlaveListener> listeners = new ConcurrentHashMap<>();
    ConcurrentMap<String, Boolean> threadController = new ConcurrentHashMap<>();
    ConcurrentMap<String, Thread> masterSlaveThreads = new ConcurrentHashMap<>();


    /**
     * starts a new thread to control master/slave state.
     * this method is idempotent and if you call it over and over with the same jobName - nothing will happen
     * @param jobName - logical jobName of the work unit. you can call this method multiple times with different jobNames and listener and each unique call will create a new thread
     * @param masterSlaveListener - the listener you implement to get callbacks
     */
    public void addMasterSlaveListener(String jobName, MasterSlaveListener masterSlaveListener) {
        MasterSlaveListener existingListener = listeners.putIfAbsent(jobName, masterSlaveListener);
        if (existingListener == null) {
            startMasterSlaveThread(jobName, masterSlaveListener);
        }
    }

    /**
     * @param jobName - the jobName
     * @return true if there is a thread associated with this jobName and it is alive
     */
    public boolean isAlive(String jobName) {
        if(masterSlaveThreads.containsKey(jobName)){
            return masterSlaveThreads.get(jobName).isAlive();
        }else{
            return false;
        }
    }

    private void startMasterSlaveThread(final String jobName, final MasterSlaveListener masterSlaveListener) {
        Thread masterSlaveThread = new Thread(new MasterSlaveRunnable(jobName, masterSlaveListener), jobName + "_MasterSlaveThread");
        masterSlaveThread.setDaemon(true);
        masterSlaveThread.start();

        masterSlaveThreads.put(jobName,masterSlaveThread);
        threadController.put(jobName, Boolean.TRUE);

        masterSlaveThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.error("Error running master slave thread for Job: {} . error is: {}", jobName, e, e);
            }
        });

    }



    /**
     * remove a listener and stop its thread. calling this method will revert the work done in the #addMasterSlaveListener method
     * @param jobName logical jobName of the work unit
     * @return true if successful. will return false if this method was called without a prior listener being added to the registry or if this moethod is called more than once.
     */
    public boolean removeMasterSlaveListener(String jobName) {
        threadController.put(jobName, Boolean.FALSE);
        masterSlaveThreads.remove(jobName);
        return listeners.remove(jobName) != null;
    }


}



