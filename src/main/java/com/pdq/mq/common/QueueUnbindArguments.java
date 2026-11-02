package com.pdq.mq.common;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class QueueUnbindArguments extends BasicArguments implements Serializable {
    private String exchangeName;
    private String queueName;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
