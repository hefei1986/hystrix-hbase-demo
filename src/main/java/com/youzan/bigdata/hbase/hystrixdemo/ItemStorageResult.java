package com.youzan.bigdata.hbase.hystrixdemo;

public class ItemStorageResult <T> {

    private T result;
    private boolean success;
    private String msg;
    private Exception lastException;

    public ItemStorageResult(T result, boolean success, String msg, Exception lastException) {
        this.result = result;
        this.success = success;
        this.msg = msg;
        this.lastException = lastException;
    }

    public T getResult() {
        return result;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMsg() {
        return msg;
    }

    public Exception getLastException() {
        return lastException;
    }
}
