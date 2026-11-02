package com.pdq.mq.mqserver.dao;

import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.core.Binding;
import com.pdq.mq.mqserver.core.Exchange;
import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @Description: 使用这个类统一管理所有存储在硬盘上的数据
 *            包含两部分
 *            1.数据库管理的部分：交换机、队列、绑定
 *            2.数据文件管理的部分：消息
 *
 *       上层逻辑如果需要操作硬盘，统一调用这个类来进行操作
 * @Auther: pdq
 */
public class DiskDataCenter {
    //用这个实例来管理数据库中的数据
    private DataBaseManager dataBaseManager = new DataBaseManager();
    //用这个实例来管理数据文件中的数据
    private MessageFileManager messageFileManager = new MessageFileManager();
    //初始化操作
    public void init() {
        //对数据库操作进行初始化准备
        dataBaseManager.init();
        //对数据文件操作进行初始化准备，空的方法，放在这里，以备后续扩展使用
        messageFileManager.init();
    }

    //封装交换机操作
    public void insertExchange(Exchange exchange) {
        dataBaseManager.insertExchange(exchange);
    }
    public void deleteExchange(String exchangeName) {
        dataBaseManager.deleteExchange(exchangeName);
    }
    public List<Exchange> selectAllExchange() {
        return dataBaseManager.selectAllExchange();
    }
    //封装队列操作
    public void insertQueue(MSGQueue queue) throws IOException {
        dataBaseManager.insertQueue(queue);
        //往数据库写入队列的同时，还要在硬盘上创建相应的目录和文件
        messageFileManager.createQueueFiles(queue.getName());
    }
    public void deleteQueue(String queueName) {
        dataBaseManager.deleteQueue(queueName);
        //从数据库删除对立的同时，还要在硬盘上删除相应的目录和文件
        messageFileManager.destroyQueueFiles(queueName);
    }
    public List<MSGQueue> selectAllQueue() {
        return dataBaseManager.selectAllQueue();
    }
    //封装绑定操作
    public void insertBinding(Binding binding) {
        dataBaseManager.insertBinding(binding);
    }
    public void deleteBinding(Binding binding) {
        dataBaseManager.deleteBinding(binding);
    }
    public List<Binding> selectAllBinding() {
        return dataBaseManager.selectAllBinding();
    }
    //封装发送消息操作
    public void sendMessage(MSGQueue queue, Message message) throws IOException, MqException {
        messageFileManager.sendMessage(queue,message);
    }
    //删除消息
    public void deleteMessage(MSGQueue queue,Message message) throws IOException, ClassNotFoundException, MqException {
        messageFileManager.deleteMessage(queue,message);
        if(messageFileManager.checkGC(queue)) {
            messageFileManager.gcQueue(queue);
        }
    }
    //获取队列所有消息
    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException, MqException {
        return messageFileManager.loadAllMessageFromQueue(queueName);
    }
}
