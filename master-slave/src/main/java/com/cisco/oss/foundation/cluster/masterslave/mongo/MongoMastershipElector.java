package com.cisco.oss.foundation.cluster.masterslave.mongo;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.cisco.oss.foundation.cluster.masterslave.MastershipElector;
import com.cisco.oss.foundation.cluster.mongo.MongoClient;
import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.cisco.oss.foundation.configuration.CcpConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mongo implementation for logic for electing new masters.
 * Created by Yair Ogen (yaogen) on 14/02/2016.
 */
public class MongoMastershipElector implements MastershipElector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoMastershipElector.class);
    public static final String LEASE_RENEWED = "leaseRenewed";
    public static final String ACTIVE_VERSION = "activeVersion";
    public static final String ACTIVE_DATACENTER = "activeDatacenter";
    public static final String MASTER_INSTANCE_ID = "masterInstanceId";
    public static final String COMPONENT = "component";
    public static final String JOB = "job";


    private MongoClient mongoClient = MongoClient.INSTANCE;
    public static final String ID = "_id";
    private MongoCollection masterSlaveCollection = mongoClient.getMasterSlaveCollection();
    private Document document = null;
    private String id = null;
    private String jobName = null;
    private int masterSlaveLeaseTime = -1;


    @Override
    public void init(String id, String jobName) {
        this.id = id;
        this.jobName = jobName;
        this.masterSlaveLeaseTime = MasterSlaveConfigurationUtil.getMasterSlaveLeaseTime(jobName);
        if (isReady()) {
            document = masterSlaveCollection.findOne(QueryBuilder.where(ID).equals(this.id));
            if (document == null) {
                document = createNewDocument();
            }
        }
    }

    @Override
    public boolean isReady() {
        return MongoClient.INSTANCE.IS_DB_UP.get();
    }

    @Override
    public boolean isActiveVersion(String currentVersion) {

        document = masterSlaveCollection.findOne(QueryBuilder.where(ID).equals(this.id));
        if (document == null) {
            document = createNewDocument();
        }
        LOGGER.trace("document in DB: {}", document);
        Element activeVersionField = document.get(ACTIVE_VERSION);
        String activeVersion = activeVersionField != null ? activeVersionField.getValueAsString() : null;

        //if we don't need to be single across versions - we don't care what is the active version
        if (StringUtils.isNotBlank(activeVersion) && MasterSlaveConfigurationUtil.isSingleAcrossVersion(jobName)) {
            return currentVersion != null && currentVersion.equals(activeVersion);
        }

        return true;
    }

    @Override
    public boolean isActiveDataCenter(String currentDataCenter) {
        //TODO do we want to prevent having multiple datacenter documents in this collection
        MongoCollection dataCenterCollection = MongoClient.INSTANCE.getDataCenterCollection();
        ConditionBuilder datacenterQuery = QueryBuilder.where(ACTIVE_DATACENTER).equals(currentDataCenter);
        Document document = dataCenterCollection.findOne(datacenterQuery);
        return document != null;
    }

    @Override
    public boolean isMaster() {
        long leaseRenewed = System.currentTimeMillis();
        DocumentBuilder documentbuilder = new DocumentBuilderImpl(document);
        documentbuilder.remove(MASTER_INSTANCE_ID);
        documentbuilder.add(MASTER_INSTANCE_ID, MasterSlaveConfigurationUtil.INSTANCE_ID);
        documentbuilder.remove(LEASE_RENEWED);
        documentbuilder.add(LEASE_RENEWED, leaseRenewed);
        document = documentbuilder.build();

//        long lastExpectedLeaseUpdateTime = leaseRenewed - masterSlaveLeaseTime * 1000;
//        ConditionBuilder timeQuery = QueryBuilder.where(LEASE_RENEWED).lessThanOrEqualTo(lastExpectedLeaseUpdateTime);
        long lastExpectedLeaseUpdateTime = leaseRenewed - masterSlaveLeaseTime * 1000;

        ConditionBuilder instanceIdEqaulity = QueryBuilder.where(MASTER_INSTANCE_ID).equals(MasterSlaveConfigurationUtil.INSTANCE_ID);
        ConditionBuilder leaseCondition = QueryBuilder.where(LEASE_RENEWED).greaterThan(lastExpectedLeaseUpdateTime);

        Document first = QueryBuilder.and(instanceIdEqaulity, leaseCondition);
        ConditionBuilder second = QueryBuilder.where(LEASE_RENEWED).lessThanOrEqualTo(lastExpectedLeaseUpdateTime);

        Document query = QueryBuilder.or(first, second);
        Document updateLeaseQuery = QueryBuilder.and(QueryBuilder.where(ID).equals(this.id), query);

        LOGGER.trace("id: {}, leaseRenewed: {}, lease-time: {}, lastExpectedLeaseUpdateTime: {}", id, leaseRenewed, masterSlaveLeaseTime, lastExpectedLeaseUpdateTime);
//                    long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, document,false, true);
        long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, this.document);

        return numOfRowsUpdated > 0;
    }

    @Override
    public void close() {
        MongoCollection masterSlaveCollection = MongoClient.INSTANCE.getMasterSlaveCollection();
        masterSlaveCollection.delete(QueryBuilder.where("instanceId").equals(MasterSlaveConfigurationUtil.INSTANCE_ID));
        document = null;
    }

    public Document createNewDocument() {
        Document document;
        DocumentBuilder documentbuilder = new DocumentBuilderImpl();
        documentbuilder.add(ID, this.id);
        documentbuilder.add(MASTER_INSTANCE_ID, MasterSlaveConfigurationUtil.INSTANCE_ID);
        documentbuilder.add(COMPONENT, MasterSlaveConfigurationUtil.COMPONENT_NAME);
        documentbuilder.add(JOB, jobName);
        documentbuilder.add(LEASE_RENEWED, 0);
        document = documentbuilder.build();
        masterSlaveCollection.insert(documentbuilder);
        return document;
    }

    @Override
    public String getActiveVersion() {
        return System.getenv(CcpConstants.ARTIFACT_VERSION);
    }

    @Override
    public void cleanupMaster() {

    }
}
