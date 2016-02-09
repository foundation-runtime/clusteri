package com.cisco.oss.foundation.cluster.mongo;

/**
 * Created by Yair Ogen (yaogen) on 09/02/2016.
 */
public class MissingMongoConfigException extends IllegalArgumentException {
    public MissingMongoConfigException(String s) {
        super(s);
    }
}
