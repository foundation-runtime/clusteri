package com.cisco.oss.foundation.cluster.registry;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.cisco.oss.foundation.cluster.mongo.MongoClient;
import com.cisco.oss.foundation.cluster.utils.ConfigurationUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by Yair Ogen (yaogen) on 14/01/2016.
 */
public enum MasterSlaveRegistry {

    INSTANCE;

    private ConcurrentMap<String,MasterSlaveListener> listeners = new ConcurrentHashMap<>();
    public ConcurrentMap<String,Boolean> threadController = new ConcurrentHashMap<>();

    public void addMasterSlaveListener(String name, MasterSlaveListener masterSlaveListener){
        MasterSlaveListener existingListener = listeners.putIfAbsent(name, masterSlaveListener);
        if(existingListener == null){
            startMasterSlaveThread(name, masterSlaveListener);
        }
    }

    private void startMasterSlaveThread(final String name, final MasterSlaveListener masterSlaveListener) {
        Thread masterSlaveThread = new Thread(new MasterSlaveRunnable(name, masterSlaveListener), name+"_MasterSlaveThread");
        masterSlaveThread.setDaemon(true);
        masterSlaveThread.start();
        threadController.put(name,Boolean.TRUE);
//        masterSlaveThread.setUncaughtExceptionHandler();
    }

    public boolean removeMasterSlaveListener(String name){
        threadController.put(name, Boolean.FALSE);
        return listeners.remove(name) != null;
    }


    private static class MasterSlaveRunnable implements Runnable{
        private static final Logger LOGGER = LoggerFactory.getLogger(MasterSlaveRunnable.class);
        private String name;
        private MasterSlaveListener masterSlaveListener;

        public MasterSlaveRunnable(String name, MasterSlaveListener masterSlaveListener) {
            this.name = name;
            this.masterSlaveListener = masterSlaveListener;
        }

        @Override
        public void run() {

            String id = ConfigurationUtil.getComponentName() + "-" + name;
            String instanceId = ConfigurationUtil.INSTANCE_ID;

            Boolean runThread = MasterSlaveRegistry.INSTANCE.threadController.getOrDefault(name,Boolean.TRUE);

            while(runThread){

                try{

                    boolean isActiveDC = isActiveDC();

                    if (isActiveDC) {

                        long timestamp = System.currentTimeMillis();

                        MongoCollection masterSlaveCollection = MongoClient.INSTANCE.getMasterSlaveCollection();
                        Find findById = Find.builder().query(QueryBuilder.where("_id").equals(id)).build();
                        Document document = masterSlaveCollection.findOne(findById);
                        if(document == null){
                            DocumentBuilder documentbuilder = new DocumentBuilderImpl();
                            documentbuilder.add("_id",id);
                            documentbuilder.add("instanceId",instanceId);
                            documentbuilder.add("timestamp",0);

                            masterSlaveCollection.insert(documentbuilder);
                        }else{
                            DocumentBuilder documentbuilder = new DocumentBuilderImpl(document);
                            documentbuilder.remove("instanceId");
                            documentbuilder.add("instanceId",instanceId);
                            documentbuilder.remove("timestamp");
                            documentbuilder.add("timestamp",timestamp);
                            document = documentbuilder.build();
                        }

                        ConditionBuilder timeQuery = QueryBuilder.where("timestamp").lessThanOrEqualTo(timestamp - ConfigurationUtil.getMasterSlaveLeaseTime(name) * 1000);
                        Document updateLeaseQuery = QueryBuilder.and(QueryBuilder.where("_id").equals(id), timeQuery);

                        long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, document);

                        if(numOfRowsUpdated > 0){
                            LOGGER.info("{} is now master", id);
                            masterSlaveListener.goMaster();
                        }else{
                            LOGGER.info("{} is now slave", id);
                            masterSlaveListener.goSlave();
                        }
                    }

                    TimeUnit.MILLISECONDS.sleep(ConfigurationUtil.getMasterSlaveLeaseTime(name)*1000-200);

                }catch (Exception e){
                    LOGGER.error("problem running maser slave thread for: {}. error is: {}", name, e, e);
                }finally{
                    runThread = MasterSlaveRegistry.INSTANCE.threadController.getOrDefault(name,Boolean.TRUE);
                }

            }



        }

        private boolean isActiveDC() {
            String currentDC = ConfigurationUtil.ACTIVE_DATA_CENTER;
            if(StringUtils.isBlank(currentDC)){
                return true;
            }else{
                //TODO do we want to prevent having multiple datacenter documents in this collection
                MongoCollection dataCenterCollection = MongoClient.INSTANCE.getDataCenterCollection();
                ConditionBuilder datacenterQuery = QueryBuilder.where("datacenter").equals(currentDC);
                Document document = dataCenterCollection.findOne(datacenterQuery);
                return document != null;
            }
        }
    }



}
