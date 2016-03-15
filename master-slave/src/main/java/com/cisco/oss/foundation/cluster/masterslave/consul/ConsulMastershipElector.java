package com.cisco.oss.foundation.cluster.masterslave.consul;

import com.cisco.oss.foundation.cluster.masterslave.MastershipElector;
import com.cisco.oss.foundation.cluster.utils.MasterSlaveConfigurationUtil;
import com.cisco.oss.foundation.configuration.ConfigurationFactory;
import com.cisco.oss.foundation.http.HttpClient;
import com.cisco.oss.foundation.http.HttpMethod;
import com.cisco.oss.foundation.http.HttpRequest;
import com.cisco.oss.foundation.http.HttpResponse;
import com.cisco.oss.foundation.http.apache.ApacheHttpClientFactory;
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.commons.lang3.StringUtils;
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
    private HttpClient consulClient;
    private String activeVersionKey = "";
    private String sessionId = "";
    private String mastershipKey = "";
    private String jobName;
    private Thread sessionTTlThread;
    private String checkId;
    private int ttlUpdateTime;
    private Configuration conf = Configuration.defaultConfiguration();
    private Configuration nullableConf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

    @Override
    public void init(String id, String jobName) {
        this.mastershipKey = id;
        this.jobName = jobName;
        this.activeVersionKey = MasterSlaveConfigurationUtil.COMPONENT_NAME + "-version";
        HostAndPort consulHostAndPort = MasterSlaveConfigurationUtil.getConsulHostAndPort(jobName);
//        try {
        initConsul(consulHostAndPort);
//        } catch (ConsulException e) {
//            infiniteConnect(consulHostAndPort, jobName);
//        }
    }

    private void initConsul(HostAndPort consulHostAndPort) {

        ConfigurationFactory.getConfiguration().setProperty("consulClient.1.host", consulHostAndPort.getHostText());
        ConfigurationFactory.getConfiguration().setProperty("consulClient.1.port", consulHostAndPort.getPort());
        ConfigurationFactory.getConfiguration().setProperty("consulClient.http.waitingTime", "0");
        ConfigurationFactory.getConfiguration().setProperty("consulClient.http.exposeStatisticsToMonitor", "false");
        consulClient = ApacheHttpClientFactory.createHttpClient("consulClient");

        registerCheck();


        startSessionHeartbeatThread(ttlUpdateTime);
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            //ignore
        }

        createSession();
    }

    private void registerCheck() {
        checkId = mastershipKey + "-TTLCheck";
        int ttlPeriod = MasterSlaveConfigurationUtil.getMasterSlaveLeaseTime(jobName);
        ttlUpdateTime = ttlPeriod / 3;

        String ttlCheck = "{\n" +
                "    \"ID\": \"" + checkId + "\",\n" +
                "    \"Name\": \"" + mastershipKey + " Status\",\n" +
                "    \"Notes\": \"background process does a curl internally every " + ttlUpdateTime + " seconds\",\n" +
                "    \"TTL\": \"" + ttlPeriod + "s\"\n" +
                "}";

        HttpRequest registerCheck = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.PUT)
                .uri("/v1/agent/check/register")
                .entity(ttlCheck)
                .build();

        execute(registerCheck,true,"register health check");

    }

    private void closeSession() {

        HttpRequest destroySession = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.PUT)
                .uri("/v1/session/destroy/" + sessionId)
                .build();

        execute(destroySession,false,"destroy session");
    }

    private void createSession() {

        String body = "{\n" +
                "  \"Name\": \"" + MasterSlaveConfigurationUtil.INSTANCE_ID + "\",\n" +
                "  \"Checks\": [\"" + checkId + "\", \"serfHealth\"]\n" +
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
    }

    private void startSessionHeartbeatThread(int ttlUpdateTime) {
        sessionTTlThread = new Thread(() -> {
            while (true) {
                try {
                    HttpRequest passCheck = HttpRequest.newBuilder()
                            .httpMethod(HttpMethod.GET)
                            .uri("/v1/agent/check/pass/" + checkId)
                            .silentLogging()
                            .build();

                    HttpResponse response = consulClient.execute(passCheck);
                    if (!response.isSuccess()) {
                        String passCheckResponse = response.getResponseAsString();
                        LOGGER.error("failed to pass check. got response: {}, error response: {}", response.getStatus(), passCheckResponse);
                        if(StringUtils.isNotEmpty(passCheckResponse) && passCheckResponse.contains("CheckID does not have associated TTL")){
                            registerCheck();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("problem in heartbeat: {}", e);
                }finally{
                    try {
                        TimeUnit.SECONDS.sleep(ttlUpdateTime);
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
            }
        }, checkId + "Thread");
        sessionTTlThread.setDaemon(true);
        sessionTTlThread.start();
    }


    @Override
    public boolean isReady() {
        HttpRequest ping = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.GET)
                .uri("/v1/agent/self")
                .silentLogging()
                .build();


        HttpResponse response = execute(ping,false,"ping agent");

        return response.isSuccess();


    }


    public boolean isActiveKeyValue(String key, String currentValue) {

        HttpRequest getActiveKey = HttpRequest.newBuilder()
                .httpMethod(HttpMethod.GET)
                .uri("/v1/kv/" + key)
                .silentLogging()
                .build();

        HttpResponse response = consulClient.execute(getActiveKey);
        if (!response.isSuccess()) {
            LOGGER.debug("failed to get value from KV store. got response: {}, error response: {}", response.getStatus(), response.getResponseAsString());
        }else{
            String jsonKeyValue = response.getResponseAsString();
            String valueInBase64 = JsonPath.parse(jsonKeyValue).read("$.[0].Value");

            if(StringUtils.isNotEmpty(valueInBase64)){
                String value = new String(BaseEncoding.base64().decode(valueInBase64));
                return currentValue.equals(value);
            }
        }
//
        return true;
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
            LOGGER.error("failed to acquire lock. got response: {}, error response: {}", response.getStatus(), responseAsString);

            if (responseAsString.contains("invalid session")) {
                closeSession();
                createSession();

                acquireLock = HttpRequest.newBuilder()
                        .httpMethod(HttpMethod.PUT)
                        .uri("/v1/kv/" + mastershipKey)
                        .queryParams("acquire", sessionId)
                        .silentLogging()
                        .build();

                HttpResponse acquireLockRetryResponse = execute(acquireLock, false, "acquire lock");
                if(acquireLockRetryResponse.isSuccess()){
                    lockAcquired = Boolean.valueOf(acquireLockRetryResponse.getResponseAsString());
                }
            }
        }else{
            lockAcquired = Boolean.valueOf(responseAsString);
        }

        LOGGER.info("lock acquired: {}", lockAcquired);
        return lockAcquired;
    }

    @Override
    public void close() {
        if (consulClient != null) {
            HttpRequest releaseLock = HttpRequest.newBuilder()
                    .httpMethod(HttpMethod.PUT)
                    .uri("/v1/kv/" + mastershipKey)
                    .queryParams("release", sessionId)
                    .build();

            execute(releaseLock,false,"release lock");
            closeSession();
        }
    }

    private HttpResponse execute(HttpRequest request, boolean throwOnError, String opName){
        HttpResponse response = consulClient.execute(request);
        String responseAsString = response.getResponseAsString();
        if (!response.isSuccess()) {
            LOGGER.error("failed to "+opName+". got response: {}, error response: {}", response.getStatus(), responseAsString);
            if(throwOnError){
                throw new ConsulException("failed to "+opName+". status: " + response.getStatus());
            }
        }

        return response;
    }
}
