package com.youzan.bigdata.hbase.hystrixdemo.impl;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.youzan.bigdata.hbase.hystrixdemo.Item;
import com.youzan.bigdata.hbase.hystrixdemo.ItemStorage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HystrixHBaseItemStorage implements ItemStorage {

    private static final String ITEM_TABLE = "item_table";
    private static final String COLUMN_FAMILY = "cf";

    private Connection masterConn;

    private Connection slaveConn;

    private Configuration masterConfiguration;

    private Configuration slaveConfiguration;

    class HyStrixGetCommand extends HystrixCommand<Item> {

        private String id;
        private Connection masterConn;
        private Connection slaveConn;

        public HyStrixGetCommand(String id, Connection masterConn, Connection slaveConn) {
            super(HystrixCommandGroupKey.Factory.asKey("HBaseAccess"));
            this.id = id;
            this.masterConn = masterConn;
            this.slaveConn = slaveConn;
        }


        protected Item run() throws Exception {
            Table itemTable = this.masterConn.getTable(TableName.valueOf(ITEM_TABLE));
            try {
                Get get = this.generateGetObj(this.id);
                Result r = itemTable.get(get);

                if (r == null) {
                    return null;
                }

                byte[] nameBytes = r.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("name"));

                if (nameBytes == null) {
                    throw new Exception("Column 'name' not found for item:" + this.id);
                }
                return new Item(this.id, Bytes.toString(nameBytes));
            } finally {
                if (itemTable != null) {
                    try {
                        itemTable.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }


        private Get generateGetObj(String id) {
            return new Get(Bytes.toBytes(id));
        }
    }

    class HyStrixPutCommand extends HystrixCommand<Object> {

        private Item item;
        private Connection masterConn;
        private Connection slaveConn;

        public HyStrixPutCommand(Item item, Connection masterConn, Connection slaveConn) {
            super(HystrixCommandGroupKey.Factory.asKey("HBaseAccess"));
            this.item = item;
            this.masterConn = masterConn;
            this.slaveConn = slaveConn;
        }


        protected Object run() throws Exception {
            Table itemTable = this.masterConn.getTable(TableName.valueOf(ITEM_TABLE));
            try {
                itemTable.put(this.generatePutObj(this.item));
            } finally {
                if (itemTable != null) {
                    try {
                        itemTable.close();
                    } catch (IOException e) {
                        //ignore
                    }
                }
            }
            return null;
        }

        private Put generatePutObj(Item item) {
            Put put = new Put(Bytes.toBytes(item.getId()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("name"), Bytes.toBytes(item.getName()));
            return put;
        }
    }

    /**
     * @param masterConfiguration
     * @param slaveConfiguration
     */
    public HystrixHBaseItemStorage(Configuration masterConfiguration, Configuration slaveConfiguration) {
        this.masterConfiguration = masterConfiguration;
        this.slaveConfiguration = slaveConfiguration;
    }

    public void init() throws IOException {
        this.masterConn = ConnectionFactory.createConnection(this.masterConfiguration);
        if (this.slaveConfiguration != null) {
            this.slaveConn = ConnectionFactory.createConnection(this.slaveConfiguration);
        }
    }

    public void insert(Item item) throws IOException {
        new HyStrixPutCommand(item, this.masterConn, this.slaveConn).execute();
    }

    public Item getById(String id) throws IOException {
        return new HyStrixGetCommand(id, this.masterConn, this.slaveConn).execute();
    }
}
