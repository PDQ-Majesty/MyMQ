package com.pdq.mq.common;

import com.pdq.mq.mqserver.core.BasicProperties;

/**
 * @Description: 这个类表示 返回给订阅者要消费的消息
 * @Auther: pdq
 */
public class SubScribeReturns extends BasicReturns{
    private String consumerTag;
    private BasicProperties basicProperties;
    private byte[]body;

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
