package com.cisco.oss.foundation.cluster.registry;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Yair Ogen (yaogen) on 24/01/2016.
 */
public class MasterSlaveRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterSlaveRunnable.class);
    private String name;
    private MasterSlaveListener masterSlaveListener;
    private String id = null;
    private final String instanceId = ConfigurationUtil.INSTANCE_ID;
    private MongoClient mongoClient = MongoClient.INSTANCE;

    public MasterSlaveRunnable(String name, MasterSlaveListener masterSlaveListener) {
        this.name = name;
        this.masterSlaveListener = masterSlaveListener;
        this.id = ConfigurationUtil.COMPONENT_NAME + "-" + name;
    }

    @Override
    public void run() {

        //sleep a bit until the async load and connection to DB is finished.
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            LOGGER.error("INTERRUPTED");
        }

        Boolean runThread = MasterSlaveRegistry.INSTANCE.threadController.getOrDefault(name, Boolean.TRUE);

        while (runThread) {

            try {

                boolean isActiveDC = isActiveDC();

                int masterSlaveLeaseTime = ConfigurationUtil.getMasterSlaveLeaseTime(name);
                if (isActiveDC) {

                    long timestamp = System.currentTimeMillis();


                    MongoCollection masterSlaveCollection = mongoClient.getMasterSlaveCollection();
                    Document document = masterSlaveCollection.findOne(QueryBuilder.where("_id").equals(this.id));
                    String documentId = null;

                    if (document == null) {
                        DocumentBuilder documentbuilder = new DocumentBuilderImpl();
                        documentbuilder.add("_id", this.id);
                        documentbuilder.add("instanceId", instanceId);
                        documentbuilder.add("timestamp", 0);
                        document = documentbuilder.build();
                        masterSlaveCollection.insert(documentbuilder);
                    } else {
                        LOGGER.trace("document in DB: {}", document);
                        documentId = document.get("_id").getValueAsString();
                        DocumentBuilder documentbuilder = new DocumentBuilderImpl(document);
                        documentbuilder.remove("instanceId");
                        documentbuilder.add("instanceId", instanceId);
                        documentbuilder.remove("timestamp");
                        documentbuilder.add("timestamp", timestamp);
                        document = documentbuilder.build();
                    }

                    ConditionBuilder timeQuery = QueryBuilder.where("timestamp").lessThanOrEqualTo(timestamp - masterSlaveLeaseTime * 1000);
                    Document updateLeaseQuery = QueryBuilder.and(QueryBuilder.where("_id").equals(this.id), timeQuery);

                    LOGGER.trace("id: {}, timestamp: {}, lease-time: {}, updateQuery: {}", id, timestamp, masterSlaveLeaseTime, updateLeaseQuery);
                    long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, document,false, true);

                    LOGGER.trace("document id: {}, my id: {}", documentId, id);
                    if (numOfRowsUpdated > 0) {
                        Boolean isFirstTime = MasterSlaveRegistry.INSTANCE.firstTimeIndicator.get(name);
                        if (!id.equals(documentId) || isFirstTime.booleanValue()) {
                            LOGGER.info("{} is now master", this.id);
                            MasterSlaveRegistry.INSTANCE.firstTimeIndicator.put(name, Boolean.FALSE);
                            masterSlaveListener.goMaster();
                        }
                    } else {
                        if (documentId == null || id.equals(documentId)) {
                            LOGGER.info("{} is now slave", this.id);
                            masterSlaveListener.goSlave();
                        }
                    }
                }

                TimeUnit.MILLISECONDS.sleep(masterSlaveLeaseTime * 1000);

            } catch (Exception e) {
                LOGGER.error("problem running maser slave thread for: {}. error is: {}", name, e, e);
            } finally {
                runThread = MasterSlaveRegistry.INSTANCE.threadController.getOrDefault(name, Boolean.TRUE);
            }

        }
    }

    private boolean isActiveDC() {
        String currentDC = ConfigurationUtil.ACTIVE_DATA_CENTER;
        if (StringUtils.isBlank(currentDC)) {
            return true;
        } else {
            //TODO do we want to prevent having multiple datacenter documents in this collection
            MongoCollection dataCenterCollection = MongoClient.INSTANCE.getDataCenterCollection();
            ConditionBuilder datacenterQuery = QueryBuilder.where("datacenter").equals(currentDC);
            Document document = dataCenterCollection.findOne(datacenterQuery);
            return document != null;
        }
    }

}
