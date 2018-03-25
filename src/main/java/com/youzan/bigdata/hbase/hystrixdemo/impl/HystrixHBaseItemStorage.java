package com.youzan.bigdata.hbase.hystrixdemo.impl;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
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

    class HyStrixGetCommand extends HystrixCommand<ItemStorageResult<Item>> {

        private String id;
        private Connection masterConn;
        private Connection slaveConn;

        public HyStrixGetCommand(String id, Connection masterConn, Connection slaveConn) {
            super(HystrixCommandGroupKey.Factory.asKey("HBaseAccess"));
            this.id = id;
            this.masterConn = masterConn;
            this.slaveConn = slaveConn;
        }


        private ItemStorageResult<Item> fetchItem(Connection conn, String id) throws IOException {
            Table itemTable = conn.getTable(TableName.valueOf(ITEM_TABLE));
            try {
                Get get = this.generateGetObj(this.id);
                Result r = itemTable.get(get);
                if (r == null) {
                    return null;
                }
                byte[] nameBytes = r.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("name"));
                if (nameBytes == null) {
                    throw new IOException("Column 'name' not found for item:" + this.id);
                }
                return new ItemStorageResult<Item>(new Item(this.id, Bytes.toString(nameBytes)), true, "", null);
            } catch (IOException ioe) {
                if(conn == masterConn && null != slaveConn){
                    throw ioe; // retry
                } else {
                    return new ItemStorageResult<Item>(null, false, ioe.getMessage(), ioe);
                }
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

        protected ItemStorageResult<Item> run() throws Exception {
            return this.fetchItem(this.masterConn, this.id);
        }

        @Override
        protected ItemStorageResult<Item> getFallback() {
            try {
                return this.fetchItem(this.slaveConn, this.id);
            } catch(IOException ioe) {
                // not gonna happen
                return null;
            }
        }

        private Get generateGetObj(String id) {
            return new Get(Bytes.toBytes(id));
        }
    }

    class HyStrixPutCommand extends HystrixCommand<ItemStorageResult<Object>> {

        private Item item;
        private Connection masterConn;
        private Connection slaveConn;

        public HyStrixPutCommand(Item item, Connection masterConn, Connection slaveConn) {
            super(HystrixCommandGroupKey.Factory.asKey("HBaseAccess"));
            this.item = item;
            this.masterConn = masterConn;
            this.slaveConn = slaveConn;
        }

        private ItemStorageResult<Object> putItem(Connection conn) throws IOException {
            Table itemTable = conn.getTable(TableName.valueOf(ITEM_TABLE));
            try {
                itemTable.put(this.generatePutObj(this.item));
            } catch (IOException ioe) {
                if(conn == masterConn && slaveConn != null) {
                    throw ioe;
                } else {
                    return new ItemStorageResult<Object>(null, false, ioe.getMessage(), ioe);
                }
            } finally {
                if (itemTable != null) {
                    try {
                        itemTable.close();
                    } catch (IOException e) {
                        //ignore
                    }
                }
            }
            return new ItemStorageResult<Object>(null, true, "", null);
        }

        protected ItemStorageResult<Object> run() throws Exception {
            return this.putItem(this.masterConn);
        }

        @Override
        protected ItemStorageResult<Object> getFallback() {
            ItemStorageResult<Object> result = null;
            try {
                result = this.putItem(this.slaveConn);
            } catch(IOException ioe) {
                // not gonna happen
            }
            return  result;
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
        ItemStorageResult<Object> result = new HyStrixPutCommand(item, this.masterConn, this.slaveConn).execute();
        if(!result.isSuccess()) {
            throw new IOException(result.getLastException());
        }
    }

    public Item getById(String id) throws IOException {
        ItemStorageResult<Item> itemItemStorageResult = new HyStrixGetCommand(id, this.masterConn, this.slaveConn).execute();
        if(!itemItemStorageResult.isSuccess()) {
            throw new IOException(itemItemStorageResult.getLastException());
        } else {
            return itemItemStorageResult.getResult();
        }
    }
}
