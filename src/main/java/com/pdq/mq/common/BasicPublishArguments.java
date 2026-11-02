package com.pdq.mq.common;

import com.pdq.mq.mqserver.core.BasicProperties;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class BasicPublishArguments extends BasicArguments implements Serializable {
    private String exchangeName;
    private BasicProperties basicProperties;
    private byte[] body;
    private String routingKey;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
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

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
}
