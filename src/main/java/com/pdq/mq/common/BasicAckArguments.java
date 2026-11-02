package com.pdq.mq.common;

import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.core.Message;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class BasicAckArguments extends BasicArguments implements Serializable {
    private String queueName;
    private String messageId;

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }
}
