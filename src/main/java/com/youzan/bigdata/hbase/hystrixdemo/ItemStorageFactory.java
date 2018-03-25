package com.youzan.bigdata.hbase.hystrixdemo;

import com.youzan.bigdata.hbase.hystrixdemo.impl.HystrixHBaseItemStorage;
import org.apache.hadoop.conf.Configuration;

public class ItemStorageFactory {

    private static HystrixHBaseItemStorage hystrixHBaseItemStorage;
    private static Object obj = new Object();

    public static ItemStorage getItemStorage() {
        if(hystrixHBaseItemStorage == null) {
            synchronized (obj) {
                if(hystrixHBaseItemStorage == null) {
                    hystrixHBaseItemStorage = new HystrixHBaseItemStorage(getMasterConfiguration(), getSlaveConfiguration());
                }
            }
        }
        return hystrixHBaseItemStorage;
    }

    private static Configuration getMasterConfiguration() {
        return null;
        // TODO
    }

    private static Configuration getSlaveConfiguration() {
        return null;
        // TODO
    }
}

