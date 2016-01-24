package com.cisco.oss.foundation.cluster.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Yair Ogen (yaogen) on 14/01/2016.
 */
public enum MasterSlaveRegistry {

    INSTANCE;

    private static Logger LOGGER = LoggerFactory.getLogger(MasterSlaveRegistry.class);

    private ConcurrentMap<String, MasterSlaveListener> listeners = new ConcurrentHashMap<>();
    public ConcurrentMap<String, Boolean> threadController = new ConcurrentHashMap<>();

    public void addMasterSlaveListener(String name, MasterSlaveListener masterSlaveListener) {
        MasterSlaveListener existingListener = listeners.putIfAbsent(name, masterSlaveListener);
        if (existingListener == null) {
            startMasterSlaveThread(name, masterSlaveListener);
        }
    }

    private void startMasterSlaveThread(final String name, final MasterSlaveListener masterSlaveListener) {
        Thread masterSlaveThread = new Thread(new MasterSlaveRunnable(name, masterSlaveListener), name + "_MasterSlaveThread");
        masterSlaveThread.setDaemon(true);
        masterSlaveThread.start();
        threadController.put(name, Boolean.TRUE);
        masterSlaveThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.error("Error running master slave thread for: {}. error is: {}", name, e, e);
            }
        });
    }

    public boolean removeMasterSlaveListener(String name) {
        threadController.put(name, Boolean.FALSE);
        return listeners.remove(name) != null;
    }


}



