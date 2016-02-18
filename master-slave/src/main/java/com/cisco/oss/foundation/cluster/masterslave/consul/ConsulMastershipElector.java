package com.cisco.oss.foundation.cluster.masterslave.consul;

import com.cisco.oss.foundation.cluster.masterslave.MastershipElector;
import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import com.orbitz.consul.model.session.ImmutableSession;
import com.orbitz.consul.model.session.Session;
import com.orbitz.consul.model.session.SessionCreatedResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consul implementation for logic for electing new masters.
 * Created by Yair Ogen (yaogen) on 15/02/2016.
 */
public class ConsulMastershipElector implements MastershipElector {

    public static final String ACTIVE_DATACENTER = "activeDatacenter";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulMastershipElector.class);
    private static final int CONSUL_RETRY_DELAY = 10;
    private Consul consul = null;
    private String activeVersionKey = "";
    private String sessionId = "";
    private String mastershipKey = "";
    public final AtomicBoolean IS_CONSUL_UP = new AtomicBoolean(false);


    @Override
    public void init(String id, String jobName) {
        this.mastershipKey = id;
        this.activeVersionKey = MasterSlaveConfigurationUtil.COMPONENT_NAME+"-version";
        HostAndPort consulHostAndPort = MasterSlaveConfigurationUtil.getConsulHostAndPort(jobName);
        try {
            initConsul(consulHostAndPort);
        } catch (ConsulException e) {
            infiniteConnect(consulHostAndPort);
        }
    }

    private void initConsul(HostAndPort consulHostAndPort) {

        this.consul = Consul.builder().withHostAndPort(consulHostAndPort).build();
        SessionCreatedResponse session = consul.sessionClient().createSession(ImmutableSession.builder().name(MasterSlaveConfigurationUtil.INSTANCE_ID).build());
        this.sessionId = session.getId();
    }

    private void infiniteConnect(HostAndPort consulHostAndPort) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(!IS_CONSUL_UP.get()){
                    try {
                        initConsul(consulHostAndPort);
                        IS_CONSUL_UP.set(true);
                        LOGGER.info("consul reconnect is successful");
                    } catch (Exception e) {
                        LOGGER.warn("consul reconnect failed. retrying in {} seconds. error: {}", CONSUL_RETRY_DELAY, e);
                        try {
                            TimeUnit.SECONDS.sleep(CONSUL_RETRY_DELAY);
                        } catch (InterruptedException e1) {
                            //ignore
                        }
                    }
                }
            }
        },"Infinite-Reconnect").start();
    }

    @Override
    public boolean isReady() {
        try {
            if(consul != null){
                consul.agentClient().ping();
                return true;
            }else{
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("can't ping the agent. error: {}", e);
            return false;
        }
    }

    @Override
    public boolean isActiveVersion(String currentVersion) {
        Optional<String> valueAsString = consul.keyValueClient().getValueAsString(activeVersionKey);
        if(valueAsString.isPresent()){
            return currentVersion.equals(valueAsString.get());
        }
        return true;
    }

    @Override
    public boolean isActiveDataCenter(String currentDataCenter) {
        Optional<String> valueAsString = consul.keyValueClient().getValueAsString(ACTIVE_DATACENTER);
        if(valueAsString.isPresent()){
            return currentDataCenter.equals(valueAsString.get());
        }
        return true;
    }

    @Override
    public boolean isMaster() {
        boolean lockAcquired = consul.keyValueClient().acquireLock(mastershipKey, sessionId);
        return lockAcquired;
    }

    @Override
    public void close() {
        if (consul != null) {
            consul.keyValueClient().releaseLock(mastershipKey, sessionId);
        }
    }
}
