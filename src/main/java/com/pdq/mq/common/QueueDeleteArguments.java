package com.pdq.mq.common;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class QueueDeleteArguments extends BasicArguments implements Serializable {
    private String queueName;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
