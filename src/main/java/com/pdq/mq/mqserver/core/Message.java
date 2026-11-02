package com.pdq.mq.mqserver.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

/**
 * @Description:
 * @Auther: pdq
 */
    //这个类对象要在网络上进行传输、写入文件必须进行序列化转为字节二进制数组,接收方收到这个二进制数组再进行反序列化拿到对象
    //要实现序列化 要实现一个serializable，告诉jdk这个类可以序列化
    //offsetBeg 和 offsetEnd不需要进行序列化，因为这两个属性是message持久化写入磁盘文件后，MQ服务器才会计算出的值
    //这两个属性在网络上传输也没有意义，因为接收方访问不了message所在的文件
    //只是存储在内存对象中，从内存对象中取值，然后再磁盘文件中寻找信息位置
    //transient修饰属性表示该属性不会被序列化
public class Message implements Serializable {
    //Message两个核心内容属性和消息
    //消息的属性
    private BasicProperties basicProperties = new BasicProperties();
    //消息的正文
    private byte[] body;

    //下面为信息的辅助属性
    //Message后续会存储在文件中，一个文件中会存储很多条消息
    //为了在一个文件中定位到特定的消息，使用偏移量进行表示消息位置
    //默认规则是前闭后开,[offsetBeg,offsetEnd)
    //表示消息开头距离文件的距离
    //transient修饰属性表示该属性不会被序列化
    private transient long offsetBeg = 0;
    //表示消息结尾距离文件的距离
    private transient long offsetEnd = 0;
    //表示消息在文件中是否有效,0x1表示有效,0x0表示无效
    private byte isValid = 0x1;

    //由于Message的属性有点多，所以使用工厂方法进行创建Message对象
    //相较于构造方法，工厂方法可以自定义设置方法名，可以根据方法名显式表示该方法的特性

    //此工厂方法自动生成messageId
    public static Message createMessageWithId(String routingKey,BasicProperties basicProperties,byte[] body) {
        Message message = new Message();
        if(basicProperties != null) {
            message.setBasicProperties(basicProperties);
        }
        message.setMessageId("M-"+UUID.randomUUID());
        message.setRoutingKey(routingKey);
        message.setBody(body);
        return message;
        //此处不设置offsetBeg和offsetEnd，这两个属性表示消息是持久化存储，待消息存储到磁盘文件之前再进行设置
        //而此时只是在内存中创建一个Message对象
    }
    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }
    public String getRoutingKey() {
        return getBasicProperties().getRoutingKey();
    }

    public void setRoutingKey(String routingKey) {
        getBasicProperties().setRoutingKey(routingKey);
    }
    public String getMessageId() {
        return getBasicProperties().getMessageId();
    }
    public void setMessageId(String messageId) {
        getBasicProperties().setMessageId(messageId);
    }

    public int getDeliverMode() {
        return getBasicProperties().getDeliverMode();
    }
    public void setDeliverMode(int deliverMode) {
        getBasicProperties().setDeliverMode(deliverMode);
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getOffsetBeg() {
        return offsetBeg;
    }

    public void setOffsetBeg(long offsetBeg) {
        this.offsetBeg = offsetBeg;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    public void setOffsetEnd(long offsetEnd) {
        this.offsetEnd = offsetEnd;
    }

    public byte getIsValid() {
        return isValid;
    }

    public void setIsValid(byte isValid) {
        this.isValid = isValid;
    }

    @Override
    public String toString() {
        return "Message{" +
                "basicProperties=" + basicProperties +
                ", body=" + Arrays.toString(body) +
                ", offsetBeg=" + offsetBeg +
                ", offsetEnd=" + offsetEnd +
                ", isValid=" + isValid +
                '}';
    }
}
