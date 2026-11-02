package com.pdq.mq.common;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class QueueBindArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String exchangeName;
    private String bindingKey;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}
