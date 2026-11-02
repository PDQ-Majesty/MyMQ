package com.pdq.mq.common;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class BasicConsumeArguments extends BasicArguments implements Serializable {
    private String consumeTag;
    private String queueName;
    private boolean autoAck;

    public String getConsumeTag() {
        return consumeTag;
    }

    public void setConsumeTag(String consumeTag) {
        this.consumeTag = consumeTag;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

}
