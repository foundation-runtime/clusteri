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

/**
 * Created by Yair Ogen (yaogen) on 24/01/2016.
 */
public class MasterSlaveRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterSlaveRunnable.class);
    private String name;
    private MasterSlaveListener masterSlaveListener;
    private final String id = ConfigurationUtil.COMPONENT_NAME + "-" + name;
    private final String instanceId = ConfigurationUtil.INSTANCE_ID;
    private MongoClient mongoClient = MongoClient.INSTANCE;

    public MasterSlaveRunnable(String name, MasterSlaveListener masterSlaveListener) {
        this.name = name;
        this.masterSlaveListener = masterSlaveListener;
    }

    @Override
    public void run() {


        Boolean runThread = MasterSlaveRegistry.INSTANCE.threadController.getOrDefault(name, Boolean.TRUE);

        while (runThread) {

            try {

                boolean isActiveDC = isActiveDC();

                if (isActiveDC) {

                    long timestamp = System.currentTimeMillis();


                    MongoCollection masterSlaveCollection = mongoClient.getMasterSlaveCollection();
                    Find.Builder builder1 = Find.builder();
                    Find.Builder builder = builder1.query(QueryBuilder.where("_id").equals(this.id));
                    Find findById = builder.build();
                    Document document = masterSlaveCollection.findOne(findById);
                    if (document == null) {
                        DocumentBuilder documentbuilder = new DocumentBuilderImpl();
                        documentbuilder.add("_id", this.id);
                        documentbuilder.add("instanceId", instanceId);
                        documentbuilder.add("timestamp", 0);

                        masterSlaveCollection.insert(documentbuilder);
                    } else {
                        DocumentBuilder documentbuilder = new DocumentBuilderImpl(document);
                        documentbuilder.remove("instanceId");
                        documentbuilder.add("instanceId", instanceId);
                        documentbuilder.remove("timestamp");
                        documentbuilder.add("timestamp", timestamp);
                        document = documentbuilder.build();
                    }

                    ConditionBuilder timeQuery = null;//QueryBuilder.where("timestamp").lessThanOrEqualTo(timestamp - ConfigurationUtil.getMasterSlaveLeaseTime(name) * 1000);
                    Document updateLeaseQuery = QueryBuilder.and(QueryBuilder.where("_id").equals(this.id), timeQuery);

                    long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, document);

                    if (numOfRowsUpdated > 0) {
                        LOGGER.info("{} is now master", this.id);
                        masterSlaveListener.goMaster();
                    } else {
                        LOGGER.info("{} is now slave", this.id);
                        masterSlaveListener.goSlave();
                    }
                }

//                    TimeUnit.MILLISECONDS.sleep(ConfigurationUtil.getMasterSlaveLeaseTime(name)*1000-200);

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
