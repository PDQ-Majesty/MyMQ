package com.pdq.mq.common;

/**
 * @Description: 函数式接口，表示一个消费者
 * @Auther: pdq
 */

public class ConsumerEnv {
    private String ConsumerTag;
    private String queueName;
    private boolean autoAck;
    private Consumer consumer;

    public String getConsumerTag() {
        return ConsumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        ConsumerTag = consumerTag;
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

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    public ConsumerEnv(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        ConsumerTag = consumerTag;
        this.queueName = queueName;
        this.autoAck = autoAck;
        this.consumer = consumer;
    }

}
