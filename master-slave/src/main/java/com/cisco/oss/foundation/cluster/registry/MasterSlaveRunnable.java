package com.cisco.oss.foundation.cluster.registry;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.builder.impl.DocumentBuilderImpl;
import com.allanbank.mongodb.builder.ConditionBuilder;
import com.allanbank.mongodb.builder.QueryBuilder;
import com.cisco.oss.foundation.cluster.mongo.MongoClient;
import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.cisco.oss.foundation.configuration.CcpConstants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Runnable to do all the logic of acquiring mastership.
 * There will be an insatance per logical name invoked by #MasterSlaveRegistry.addMasterSlaveListener
 * The runnable will query mongo and try to acquire a lock based on the componet-name, the logical name and the timestamp
 * Created by Yair Ogen (yaogen) on 24/01/2016.
 */
public class MasterSlaveRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MasterSlaveRunnable.class);
    public static final String ID = "_id";
    public static final String MASTER_INSTANCE_ID = "masterInstanceId";
    public static final String COMPONENT = "component";
    public static final String JOB = "job";
    public static final String LEASE_RENEWED = "leaseRenewed";
    public static final String ACTIVE_VERSION = "activeVersion";
    public static final String ACTIVE_DATACENTER = "activeDatacenter";
    private String name;
    private MasterSlaveListener masterSlaveListener;
    private String id = null;
    private final String instanceId = MasterSlaveConfigurationUtil.INSTANCE_ID;
    private MongoClient mongoClient = MongoClient.INSTANCE;
    static final ThreadLocal<Boolean> masterNextTimeInvoke = new ThreadLocal<>();
    static final ThreadLocal<Boolean> slaveNextTimeInvoke = new ThreadLocal<>();

    public MasterSlaveRunnable(String name, MasterSlaveListener masterSlaveListener) {
        this.name = name;
        this.masterSlaveListener = masterSlaveListener;
        this.id = MasterSlaveConfigurationUtil.COMPONENT_NAME + "-" + name;
    }

    @Override
    public void run() {

        masterNextTimeInvoke.set(Boolean.TRUE);
        slaveNextTimeInvoke.set(Boolean.TRUE);

        //sleep a bit until the async load and connection to DB is finished.
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            LOGGER.error("INTERRUPTED");
        }

        Boolean runThread = MasterSlaveRegistry.INSTANCE.threadController.getOrDefault(name, Boolean.TRUE);
        String artifactVersion = System.getenv(CcpConstants.ARTIFACT_VERSION);

        while (runThread) {

            int masterSlaveLeaseTime = MasterSlaveConfigurationUtil.getMasterSlaveLeaseTime(name);

            try {

                chooseMaster(artifactVersion, masterSlaveLeaseTime);

            } catch (Exception e) {
                LOGGER.warn("problem running master slave thread for: {}. RETRYING ONCE. error is: {}", name, e, e);
                try {
                    chooseMaster(artifactVersion, masterSlaveLeaseTime);
                } catch (Exception e1) {
                    LOGGER.error("problem running master slave thread for: {}. error is: {}", name, e1, e1);
                    goSlave();
                }
            } finally {
                try {
                    TimeUnit.MILLISECONDS.sleep(masterSlaveLeaseTime * 1000);
                } catch (InterruptedException e) {
                    //ignore
                }
                runThread = MasterSlaveRegistry.INSTANCE.threadController.getOrDefault(name, Boolean.TRUE);
            }

        }
    }

    private void chooseMaster(String artifactVersion, int masterSlaveLeaseTime) {

        boolean isActiveDC = isActiveDC();

        if (isActiveDC && MongoClient.INSTANCE.IS_DB_UP.get()) {

            long leaseRenewed = System.currentTimeMillis();


            MongoCollection masterSlaveCollection = mongoClient.getMasterSlaveCollection();
            Document document = masterSlaveCollection.findOne(QueryBuilder.where(ID).equals(this.id));

            //if anything fails - fallback to activeVersion is true
            boolean isActiveVersion = true;


            if (document == null) {
                document = createNewDocument(masterSlaveCollection);
            } else {
                LOGGER.trace("document in DB: {}", document);
                Element activeVersionField = document.get(ACTIVE_VERSION);
                String activeVersion = activeVersionField != null ? activeVersionField.getValueAsString() : null;

                //if we don't need to be single across versions - we don't care what is the active version
                if (StringUtils.isNotBlank(activeVersion) && MasterSlaveConfigurationUtil.isSingleAcrossVersion(name)) {
                    isActiveVersion = artifactVersion != null && artifactVersion.equals(activeVersion);
                }
                DocumentBuilder documentbuilder = new DocumentBuilderImpl(document);
                documentbuilder.remove(MASTER_INSTANCE_ID);
                documentbuilder.add(MASTER_INSTANCE_ID, instanceId);
                documentbuilder.remove(LEASE_RENEWED);
                documentbuilder.add(LEASE_RENEWED, leaseRenewed);
                document = documentbuilder.build();
            }

            if (isActiveVersion) {

                switch (MasterSlaveConfigurationUtil.getMasterSlaveMultiplicity(name)) {
                    case SINGLE: {
                        chooseMasterBasedOnLease(masterSlaveLeaseTime, leaseRenewed, masterSlaveCollection, document);
                        break;
                    }
                    case MULTI: {
                        if (masterNextTimeInvoke.get()) {
                            goMaster();
                        }
                        break;
                    }
                    default: {
                        chooseMasterBasedOnLease(masterSlaveLeaseTime, leaseRenewed, masterSlaveCollection, document);
                    }
                }


            } else if (slaveNextTimeInvoke.get()) { //Not active version
                goSlave();
            }
        } else if (!isActiveDC && slaveNextTimeInvoke.get()) { //Not active Datacenter
            goSlave();
        }
    }

    private void chooseMasterBasedOnLease(int masterSlaveLeaseTime, long leaseRenewed, MongoCollection masterSlaveCollection, Document document) {
        long lastExpectedLeaseUpdateTime = leaseRenewed - masterSlaveLeaseTime * 1000;
        ConditionBuilder timeQuery = QueryBuilder.where(LEASE_RENEWED).lessThanOrEqualTo(lastExpectedLeaseUpdateTime);
        Document updateLeaseQuery = QueryBuilder.and(QueryBuilder.where(ID).equals(this.id), timeQuery);

        LOGGER.trace("id: {}, leaseRenewed: {}, lease-time: {}, lastExpectedLeaseUpdateTime: {}", id, leaseRenewed, masterSlaveLeaseTime, lastExpectedLeaseUpdateTime);
//                    long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, document,false, true);
        long numOfRowsUpdated = masterSlaveCollection.update(updateLeaseQuery, document);

        if (numOfRowsUpdated > 0) {
            if (masterNextTimeInvoke.get()) {
                goMaster();
            }
        } else {
            if (slaveNextTimeInvoke.get()) {
                if (slaveNextTimeInvoke.get()) {
                    goSlave();
                }
            }
        }
    }

    public Document createNewDocument(MongoCollection masterSlaveCollection) {
        Document document;
        DocumentBuilder documentbuilder = new DocumentBuilderImpl();
        documentbuilder.add(ID, this.id);
        documentbuilder.add(MASTER_INSTANCE_ID, instanceId);
        documentbuilder.add(COMPONENT, MasterSlaveConfigurationUtil.COMPONENT_NAME);
        documentbuilder.add(JOB, name);
        documentbuilder.add(LEASE_RENEWED, 0);
        document = documentbuilder.build();
        masterSlaveCollection.insert(documentbuilder);
        return document;
    }

    public void goMaster() {
        LOGGER.info("{} is now master", this.instanceId);
        masterNextTimeInvoke.set(Boolean.FALSE);
        slaveNextTimeInvoke.set(Boolean.TRUE);
        masterSlaveListener.goMaster();
    }

    public void goSlave() {
        LOGGER.info("{} is now slave", this.instanceId);
        slaveNextTimeInvoke.set(Boolean.FALSE);
        masterNextTimeInvoke.set(Boolean.TRUE);
        masterSlaveListener.goSlave();
    }

    private boolean isActiveDC() {

        //if we don't need to be single across datacetners we're in active DC for all we care.
        if (!MasterSlaveConfigurationUtil.isSingleAcrossMDC(name)) {
            return true;
        }

        String currentDC = MasterSlaveConfigurationUtil.ACTIVE_DATA_CENTER;
        if (StringUtils.isBlank(currentDC)) {
            return true;
        } else {
            try {
                //TODO do we want to prevent having multiple datacenter documents in this collection
                MongoCollection dataCenterCollection = MongoClient.INSTANCE.getDataCenterCollection();
                ConditionBuilder datacenterQuery = QueryBuilder.where(ACTIVE_DATACENTER).equals(currentDC);
                Document document = dataCenterCollection.findOne(datacenterQuery);
                return document != null;
            } catch (Exception e) {
                //if this fails  for any reason we treat this as a non DC supported environment.
                LOGGER.error("problem reading datacenter collection - assuming in active DC");
                return true;
            }
        }
    }

}
