package com.cisco.oss.foundation.cluster.masterslave.consul;

/**
 * Created by Yair Ogen (yaogen) on 14/03/2016.
 */
public class ConsulException extends RuntimeException {

    public ConsulException(String message) {
        super(message);
    }

    public ConsulException(String message, Throwable cause) {
        super(message, cause);
    }
}
