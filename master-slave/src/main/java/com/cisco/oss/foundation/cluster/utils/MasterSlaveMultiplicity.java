package com.cisco.oss.foundation.cluster.utils;

/**
 * Created by Yair Ogen (yaogen) on 09/02/2016.
 */
public enum MasterSlaveMultiplicity {

    SINGLE("single"),
    MULTI("multi");

    private String multiplicity = "single";
    MasterSlaveMultiplicity(String multiplicity){
        this.multiplicity = multiplicity;
    }

    public String multiplicity(){
        return multiplicity;
    }

    public static MasterSlaveMultiplicity newInstance(String multiplicity){
        switch (multiplicity){
            case "multi":
                return MULTI;
            default:
                return SINGLE;
        }

    }
}
