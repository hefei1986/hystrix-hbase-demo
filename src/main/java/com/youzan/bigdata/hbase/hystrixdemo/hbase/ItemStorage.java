package com.youzan.bigdata.hbase.hystrixdemo.hbase;

import com.youzan.bigdata.hbase.hystrixdemo.hbase.impl.Item;

import java.io.IOException;

public interface ItemStorage {

    void insert(Item item) throws IOException;

    Item getById(String id) throws IOException;
}
