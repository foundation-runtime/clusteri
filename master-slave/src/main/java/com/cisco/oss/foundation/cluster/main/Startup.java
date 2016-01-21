package com.cisco.oss.foundation.cluster.main;

import com.allanbank.mongodb.MongoCollection;
import com.cisco.oss.foundation.cluster.mongo.MongoClient;

/**
 * Created by Yair Ogen (yaogen) on 14/01/2016.
 */
public class Startup {

    public static void main(String[] args) {
        MongoCollection dataCenterCollection = MongoClient.INSTANCE.getDataCenterCollection();
        MongoCollection masterSlaveCollection = MongoClient.INSTANCE.getMasterSlaveCollection();

    }
}
