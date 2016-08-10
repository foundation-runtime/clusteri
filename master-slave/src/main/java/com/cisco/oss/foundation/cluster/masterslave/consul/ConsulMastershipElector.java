package com.cisco.oss.foundation.cluster.masterslave.consul;

import com.cisco.oss.foundation.cluster.masterslave.MastershipElector;
import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.cisco.oss.foundation.configuration.CcpConstants;
import com.cisco.oss.foundation.configuration.ConfigurationFactory;
import com.cisco.oss.foundation.http.HttpClient;
import com.cisco.oss.foundation.http.HttpMethod;
import com.cisco.oss.foundation.http.HttpRequest;
import com.cisco.oss.foundation.http.HttpResponse;
import com.cisco.oss.foundation.http.apache.ApacheHttpClientFactory;
import com.google.common.io.BaseEncoding;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Consul implementation for logic for electing new masters.
 * Created by Yair Ogen (yaogen) on 15/02/2016.
 */
public class ConsulMastershipElector implements MastershipElector {

    public static final String ACTIVE_DATACENTER = "primaryDatacenter";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulMastershipElector.class);
    protected HttpClient consulClient;
    protected String activeVersionKey = "";
    protected String sessionId = "";
    protected String mastershipKey = "";
    protected String jobName;
    protected Thread sessionTTlThread;
    //    protected String checkId;
    protected int ttlUpdateTime;
    protected int ttlPeriod;
    protected Configuration conf = Configuration.defaultConfiguration();
    protected Configuration nullableConf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

    @Override
    public void init(String id, String jobName) {
        this.mastershipKey = "master-slave/" + MasterSlaveConfigurationUtil.COMPONENT_NAME + "/" + jobName;
        this.jobName = jobName;
        this.activeVersionKey = getActiveVersionKey();

        int numberOfInitAttempts = ConfigurationFactory.getConfiguration().getInt("consulClient.numberOfInitAttempts", 3);
        boolean success = false;

        ttlPeriod = MasterSlaveConfigurationUtil.getMasterSlaveLeaseTime(jobName);
        if (ttlPeriod < 10) {
            LOGGER.error("Lease time must be at least 10 seconds. For {} the value is: {}. Exiting the application", jobName, ttlPeriod);
            System.exit(-1);
        }
        ttlUpdateTime = ttlPeriod / 3;

        for (int attemptNumber = 1; attemptNumber <= numberOfInitAttempts && !success; attemptNumber++) {
            try {
                initConsul();
                success = true;
            } catch (Exception e) {
                if (attemptNumber != numberOfInitAttempts) {
                    LOGGER.warn("problem initializing consul elector for: {}. Attempt: {}. Error is: {}", jobName, attemptNumber, e);
                } else {
                    LOGGER.error("problem initializing consul elector for: {}. Attempt: {} Failed. EXITING the application. Error is: {}", jobName, attemptNumber, e);
                    System.exit(-1);
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e1) {
                    //ignore
                }
            }
        }
    }

    private void initConsul() {

        if (consulClient == null) {
            consulClient = ApacheHttpClientFactory.createHttpClient("consulClient");
        }

        createSession();
    }

    private void closeSession() {

        HttpRequest destroySession = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.PUT)
                .uri("/v1/session/destroy/" + sessionId)
                .build();

        execute(destroySession, false, "destroy session");
    }

    private void createSession() {

        String body = "{\n" +
                "  \"Name\": \"" + MasterSlaveConfigurationUtil.INSTANCE_ID + "\",\n" +
                "\"TTL\": \"" + ttlPeriod + "s\"" +
                "}";

        HttpRequest createSession = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.PUT)
                .uri("/v1/session/create")
                .entity(body)
                .build();

        HttpResponse response = execute(createSession, true, "create session");

        String jsonId = response.getResponseAsString();
        this.sessionId = JsonPath.parse(jsonId).read("$.ID");
        LOGGER.info("new Session Id is: {}", sessionId);

        if (sessionTTlThread == null) {
            startSessionHeartbeatThread();
        }
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            //ignore
        }
    }

    private void startSessionHeartbeatThread() {
        sessionTTlThread = new Thread(() -> {
            while (true) {
                try {
                    HttpRequest renewSession = HttpRequest.newBuilder()
                            .httpMethod(HttpMethod.PUT)
                            .uri("/v1/session/renew/" + sessionId)
                            .silentLogging()
                            .build();

                    HttpResponse response = consulClient.execute(renewSession);
                    if (!response.isSuccess()) {
                        String renewSessionResponse = response.getResponseAsString();
                        LOGGER.error("failed to pass check. got response: {}, error response: {}", response.getStatus(), renewSessionResponse);
                    }
                } catch (Exception e) {
                    LOGGER.warn("problem in heartbeat: {}", e.toString());
                } finally {
                    try {
                        TimeUnit.SECONDS.sleep(ttlUpdateTime);
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }
        }, jobName + "-SessionRenewThread");
        sessionTTlThread.setDaemon(true);
        sessionTTlThread.start();
        sessionTTlThread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                LOGGER.error("Uncaught Exception for Job: {} in thread: {}. Exception is: {}", jobName, t.getName(), e);
            }
        });
    }


    @Override
    public boolean isReady() {
        HttpRequest ping = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.GET)
                .uri("/v1/agent/self")
                .silentLogging()
                .build();


        HttpResponse response = execute(ping, false, "ping agent");

        return response.isSuccess();


    }


    public boolean isActiveKeyValue(String key, String currentValue) {

        HttpRequest getActiveKey = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.GET)
                .uri("/v1/kv/" + key)
                .silentLogging()
                .build();

        boolean isActive = true;
        String consulValue = "";

        HttpResponse response = consulClient.execute(getActiveKey);
        if (!response.isSuccess()) {
            LOGGER.debug("failed to get value from KV store for key: {}. got response: {}, error response: {}", key, response.getStatus(), response.getResponseAsString());
        } else {
            String jsonKeyValue = response.getResponseAsString();
            String valueInBase64 = JsonPath.parse(jsonKeyValue).read("$.[0].Value");

            if (StringUtils.isNotEmpty(valueInBase64)) {
                consulValue = new String(BaseEncoding.base64().decode(valueInBase64));
                isActive = currentValue.equals(consulValue);
            }
        }

        LOGGER.debug("isActive: {}, env value: {}, consul key: {}, consul value: {}", isActive, currentValue, key, consulValue);

        return isActive;
    }

    @Override
    public boolean isActiveVersion(String currentVersion) {

        return isActiveKeyValue(activeVersionKey, currentVersion);
    }

    @Override
    public boolean isActiveDataCenter(String currentDataCenter) {

        return isActiveKeyValue(ACTIVE_DATACENTER, currentDataCenter);

    }

    @Override
    public boolean isMaster() {
        boolean lockAcquired = false;

        HttpRequest getSession = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.GET)
                .uri("/v1/kv/" + mastershipKey)
                .silentLogging()
                .build();

        HttpResponse getSessionResponse = consulClient.execute(getSession);
        if (getSessionResponse.isSuccess()) {
            String sessionOwner = JsonPath.using(nullableConf).parse(getSessionResponse.getResponseAsString()).read("$.[0].Session");
            if (StringUtils.isNoneBlank(sessionOwner)) {
                return sessionId.equals(sessionOwner);
            }
        }


        HttpRequest acquireLock = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.PUT)
                .uri("/v1/kv/" + mastershipKey)
                .queryParams("acquire", sessionId)
                .silentLogging()
                .build();

        HttpResponse response = consulClient.execute(acquireLock);
        String responseAsString = response.getResponseAsString();
        if (!response.isSuccess()) {
            LOGGER.error("failed to acquire lock for key: {}. got response: {}, error response: {}", mastershipKey, response.getStatus(), responseAsString);

            if (responseAsString.contains("invalid session") || responseAsString.contains("Invalid session")) {
                closeSession();
                createSession();

                acquireLock = HttpRequest.newBuilder()
                        .httpMethod(HttpMethod.PUT)
                        .uri("/v1/kv/" + mastershipKey)
                        .queryParams("acquire", sessionId)
                        .silentLogging()
                        .build();

                HttpResponse acquireLockRetryResponse = execute(acquireLock, false, "acquire lock");
                if (acquireLockRetryResponse.isSuccess()) {
                    lockAcquired = Boolean.valueOf(acquireLockRetryResponse.getResponseAsString());
                }
            }
        } else {
            lockAcquired = Boolean.valueOf(responseAsString);
        }

        LOGGER.debug("lock acquired: {}", lockAcquired);
        return lockAcquired;
    }

    @Override
    public void close() {
        if (consulClient != null) {
            releaseLock();
            closeSession();
        }
    }

    private void releaseLock() {
        HttpRequest releaseLock = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.PUT)
                .uri("/v1/kv/" + mastershipKey)
                .queryParams("release", sessionId)
                .build();

        execute(releaseLock, false, "release lock");
    }

    private HttpResponse execute(HttpRequest request, boolean throwOnError, String opName) {
        HttpResponse response = consulClient.execute(request);
        String responseAsString = response.getResponseAsString();
        if (!response.isSuccess()) {
            LOGGER.error("failed to " + opName + ". got response: {}, error response: {}", response.getStatus(), responseAsString);
            if (throwOnError) {
                throw new ConsulException("failed to " + opName + ". status: " + response.getStatus());
            }
        }

        return response;
    }

    protected String getActiveVersionKey() {
        return MasterSlaveConfigurationUtil.COMPONENT_NAME + "-version";
    }

    @Override
    public String getActiveVersion() {
        return System.getenv(CcpConstants.ARTIFACT_VERSION);
    }

    @Override
    public void cleanupMaster() {
        try {
            releaseLock();
        } catch (Exception e) {
            LOGGER.error("instance is turning into slave but can't release lock. error: {}", e);
        }
    }
}
