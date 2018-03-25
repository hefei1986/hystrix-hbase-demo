package com.youzan.bigdata.hbase.hystrixdemo;

import com.youzan.bigdata.hbase.hystrixdemo.impl.Item;

import java.io.IOException;

public interface ItemStorage {

    void insert(Item item) throws IOException;

    Item getById(String id) throws IOException;
}
