package com.cisco.oss.foundation.cluster.test;

import com.allanbank.mongodb.bson.impl.EmptyDocument;
import com.cisco.oss.foundation.cluster.mongo.AsyncMongoClient;
import com.cisco.oss.foundation.cluster.registry.MasterSlaveListener;
import com.cisco.oss.foundation.cluster.registry.MasterSlaveRegistry;
import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.cisco.oss.foundation.configuration.CcpConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by Yair Ogen (yaogen) on 21/01/2016.
 */
public class RegistryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegistryTest.class);

    @BeforeClass
    public static void init(){
        System.setProperty(CcpConstants.RPM_SOFTWARE_NAME,"dummy_component");
        MasterSlaveConfigurationUtil.setMongodbServerConfigPrefix("mymongodb");
        List<Pair<String,Integer>> servers = new ArrayList<>();

        servers.add(Pair.of("localhost",27017));

        MasterSlaveConfigurationUtil.setMongodbServers(servers);
    }

    @Test
    public void runOnelistener(){

        CountDownLatch countDownLatch = new CountDownLatch(1);

        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test1", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST1] - I AM MASTER");
                countDownLatch.countDown();
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST1] - I AM slave");
            }
        });

        try {
            Assert.assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        }


        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test1")).count() == 1);


        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test1", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST1-1] - I AM MASTER");
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST1-1] - I AM slave");
            }
        });

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        }

        threadSet = Thread.getAllStackTraces().keySet();
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test1")).count() == 1);

    }

    @After
    public void cleanupTest(){
        MasterSlaveRegistry.INSTANCE.removeMasterSlaveListener("test1");
        MasterSlaveRegistry.INSTANCE.removeMasterSlaveListener("test2");
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        }
        AsyncMongoClient.INSTANCE.getMasterSlaveCollection().delete(EmptyDocument.INSTANCE);
        System.gc();
        MasterSlaveConfigurationUtil.setMongodbServers(new ArrayList<>());

    }

    @Test
    public void runTwolisteners(){


        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountDownLatch countDownLatch2 = new CountDownLatch(1);

        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test1", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST1] - I AM MASTER");
                countDownLatch.countDown();
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST1] - I AM slave");
            }
        });

        try {
            Assert.assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        }

        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test2", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST2] - I AM MASTER");
                countDownLatch2.countDown();
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST2] - I AM slave");
            }
        });

        try {
            Assert.assertTrue(countDownLatch2.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        }


        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test1")).count() == 1);
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test2")).count() == 1);


        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test1", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST1-1] - I AM MASTER");
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST1-1] - I AM slave");
            }
        });

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        }

        threadSet = Thread.getAllStackTraces().keySet();
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test1")).count() == 1);

    }

    @Test
    public void startAndStopListener(){
        CountDownLatch countDownLatch = new CountDownLatch(1);

        MasterSlaveRegistry.INSTANCE.addMasterSlaveListener("test1", new MasterSlaveListener() {
            @Override
            public void goMaster() {
                LOGGER.info("[TEST1] - I AM MASTER");
                countDownLatch.countDown();
            }

            @Override
            public void goSlave() {
                LOGGER.info("[TEST1] - I AM slave");
            }
        });

        try {
            Assert.assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Assert.fail(e.toString());
        }

        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test1")).count() == 1);

        System.gc();

        threadSet = Thread.getAllStackTraces().keySet();
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test1")).count() == 1);

        MasterSlaveRegistry.INSTANCE.removeMasterSlaveListener("test1");

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            LOGGER.debug("interrupted");
        }

        System.gc();

        threadSet = Thread.getAllStackTraces().keySet();
        Assert.assertTrue(threadSet.stream().filter(thread -> thread.getName().contains("test1")).count() == 0);


    }
}
