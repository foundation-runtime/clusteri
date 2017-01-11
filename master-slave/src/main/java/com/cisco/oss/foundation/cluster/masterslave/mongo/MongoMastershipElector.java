package com.cisco.oss.foundation.cluster.masterslave.mongo;


import com.cisco.oss.foundation.cluster.mongo.MongoClient;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.client.MongoCollection;
import com.cisco.oss.foundation.cluster.masterslave.MastershipElector;
import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.cisco.oss.foundation.configuration.CcpConstants;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
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
            document = (Document) masterSlaveCollection.find(new Document(ID,this.id)).limit(1).first();
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

        document = (Document) masterSlaveCollection.find(new Document(ID,this.id)).limit(1).first();
        if (document == null) {
            document = createNewDocument();
        }
        LOGGER.trace("document in DB: {}", document);
        String activeVersionField = (String) document.get(ACTIVE_VERSION);
        String activeVersion = activeVersionField != null ? activeVersionField : null;

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
        Document document = (Document) dataCenterCollection.find(new Document(ACTIVE_DATACENTER,currentDataCenter)).limit(1).first();
        return document != null;
    }

    @Override
    public boolean isMaster() {
        long leaseRenewed = System.currentTimeMillis();
        document.put(MASTER_INSTANCE_ID, MasterSlaveConfigurationUtil.INSTANCE_ID);
        document.put(LEASE_RENEWED, leaseRenewed);


//        long lastExpectedLeaseUpdateTime = leaseRenewed - masterSlaveLeaseTime * 1000;
//        ConditionBuilder timeQuery = QueryBuilder.where(LEASE_RENEWED).lessThanOrEqualTo(lastExpectedLeaseUpdateTime);
        long lastExpectedLeaseUpdateTime = leaseRenewed - masterSlaveLeaseTime * 1000;


        Bson one = Filters.eq(MASTER_INSTANCE_ID, MasterSlaveConfigurationUtil.INSTANCE_ID);
        Bson two = Filters.eq(LEASE_RENEWED, lastExpectedLeaseUpdateTime);
        Bson first = Filters.and(one, two);

//        QueryBuilder first = QueryBuilder.start(MASTER_INSTANCE_ID)
//                .is(MasterSlaveConfigurationUtil.INSTANCE_ID)
//                .and(LEASE_RENEWED).is(lastExpectedLeaseUpdateTime);

//        ConditionBuilder instanceIdEqaulity = QueryBuilder.where(MASTER_INSTANCE_ID).equals(MasterSlaveConfigurationUtil.INSTANCE_ID);
//        ConditionBuilder leaseCondition = QueryBuilder.where(LEASE_RENEWED).greaterThan(lastExpectedLeaseUpdateTime);

//        QueryBuilder.start().and(instanceIdEqaulity,leaseCondition);

//        Document first = QueryBuilder.and(instanceIdEqaulity, leaseCondition);
//        QueryBuilder second = QueryBuilder.start(LEASE_RENEWED).lessThanEquals(lastExpectedLeaseUpdateTime);
        Bson second = Filters.lte(LEASE_RENEWED,lastExpectedLeaseUpdateTime);
//        ConditionBuilder second = QueryBuilder.where(LEASE_RENEWED).lessThanOrEqualTo(lastExpectedLeaseUpdateTime);

        Bson query = Filters.or(first, second);

//        QueryBuilder query = first.or(second.get());
        Bson updateLeaseQuery = Filters.and(Filters.eq(ID, this.id), query);
//        QueryBuilder updateLeaseQuery = query.and(QueryBuilder.start(ID).is(this.id).get());


//        Document query = QueryBuilder.or(first, second);
//        Document updateLeaseQuery = QueryBuilder.and(QueryBuilder.where(ID).equals(this.id), query);

        LOGGER.trace("id: {}, leaseRenewed: {}, lease-time: {}, lastExpectedLeaseUpdateTime: {}", id, leaseRenewed, masterSlaveLeaseTime, lastExpectedLeaseUpdateTime);
//                    long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, document,false, true);
//        long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, this.document);
        Object updateDoc = masterSlaveCollection.findOneAndReplace(updateLeaseQuery, this.document);

        return updateDoc != null;
    }

    @Override
    public void close() {
        MongoCollection masterSlaveCollection = MongoClient.INSTANCE.getMasterSlaveCollection();
        masterSlaveCollection.deleteOne(new Document("instanceId",MasterSlaveConfigurationUtil.INSTANCE_ID));
        document = null;
    }

    public Document createNewDocument() {
        Document document = new Document();
        document.put(ID, this.id);
        document.put(MASTER_INSTANCE_ID, MasterSlaveConfigurationUtil.INSTANCE_ID);
        document.put(COMPONENT, MasterSlaveConfigurationUtil.COMPONENT_NAME);
        document.put(JOB, jobName);
        document.put(LEASE_RENEWED, 0);
        masterSlaveCollection.insertOne(document);
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
