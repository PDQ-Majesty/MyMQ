package com.pdq.mq.common;

import java.io.Serializable;

/**
 * @Description:  表示一个网络通信中的请求对象. 按照自定义协议的格式来展开的
 * @Auther: pdq
 */
public class Request {
    //请求的行为 (比如交换机的创建删除...)
    private int type;
    //payload的长度
    private int length;
    //请求行为的参数
    private byte[] payload;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }
}
