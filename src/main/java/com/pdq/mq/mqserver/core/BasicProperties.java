package com.pdq.mq.mqserver.core;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class BasicProperties implements Serializable {
    //表示消息的唯一身份标识,为了保证id的唯一性，使用UUID来作为messageId
    private String messageId;
    //如果当前的交换机类型是DIRECT，此时routingKey就表示要转发的队列名
    //如果当前的交换机类型是FANOUT，此时routingKey无意义
    //如果当前的交换机类型是TOPIC,此时routingKey就要和bindingKey进行匹配
    private String routingKey;
    //这个属性表示这个消息是否要持久化，1表示不持久化，2表示持久化(RabbitMQ就是这样设定的)
    private int deliverMode = 1;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public int getDeliverMode() {
        return deliverMode;
    }

    public void setDeliverMode(int deliverMode) {
        this.deliverMode = deliverMode;
    }
}
