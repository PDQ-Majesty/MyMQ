package com.pdq.mq.mqserver.core;

/**
 * @Description:这个类表示 exchange 和 MSGQueue 的关联关系
 * @Auther: pdq
 */
public class Binding {
    private String exchangeName;
    private String queueName;
    private String bindingKey;
    //这里不再加入像Exchange、MSGQueue、Message里durable、arguments等属性
    //binding是依附于Exchange和MSGQueue的，以持久化为例，如果Exchange和MSGQueue
    //都没有持久化,那么即使binding持久化了也没有意义了

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

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}
