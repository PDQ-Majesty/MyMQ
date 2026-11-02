package com.pdq.mq.mqserver.dao;

import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.core.Binding;
import com.pdq.mq.mqserver.core.Exchange;
import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: 这个类用来统一管理存储在内存中的数据
 * 主要有交换机、队列、绑定、消息
 * @Auther: pdq
 */
public class MemoryDataCenter {
    //key是exchangeName，value是Exchange对象
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();
    //key是queueName，value是MSGQueue对象
    private ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();
    //key是exchangeName，value是一个HashMap对象，这个对象的key是queueName，value是Binding对象
    private ConcurrentHashMap<String,ConcurrentHashMap<String, Binding>> bindingsMap = new ConcurrentHashMap<>();
    //key是messageId，value是Message对象
    private ConcurrentHashMap<String,Message> messageMap = new ConcurrentHashMap<>();
    //key是queueName，value是一个元素为Message的链表
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessageMap = new ConcurrentHashMap<>();
    //表示存储未被确认的消息，被消费者取走了，但未应答，“已读未回”
    //key是queueName，value是key为MessageId，value为Message对象
    //使用HashMap为了后续更好的通过MessageId定位消息，进行应答处理
    private ConcurrentHashMap<String,ConcurrentHashMap<String,Message>> messageWaitAckMap = new ConcurrentHashMap<>();
    //交换机操作
    public void insertExchange(Exchange exchange) {
        exchangeMap.put(exchange.getName(),exchange);
        System.out.println("[MemoryDataCenter] 添加交换机成功. exchangeName="+exchange.getName());
    }
    public Exchange getExchange(String exchangeName) {
        return exchangeMap.get(exchangeName);
    }
    public void deleteExchange(String exchangeName) {
        exchangeMap.remove(exchangeName);
        System.out.println("[MemoryDataCenter] 删除交换机成功. exchangeName="+exchangeName);
    }
    //队列操作
    public void insertQueue(MSGQueue queue) {
        queueMap.put(queue.getName(),queue);
        System.out.println("[MemoryDataCenter] 添加队列成功. queueName="+queue.getName());
    }
    public MSGQueue getQueue(String queueName) {
        return queueMap.get(queueName);
    }
    public void deleteQueue(String queueName) {
        queueMap.remove(queueName);
        System.out.println("[MemoryDataCenter] 删除队列成功. queueName="+queueName);
    }
    //绑定操作
    public void insertBinding(Binding binding) throws MqException {
//        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
//        //判断bindingMap是否存在，不存在就创建
//        if(bindingMap == null) {
//            bindingMap = new ConcurrentHashMap<>();
//            bindingsMap.put(binding.getExchangeName(),bindingMap);
//        }
        ConcurrentHashMap<String,Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(),k -> new ConcurrentHashMap<>());
        //再根据queueName查询一下binding是否已经存在，如果已经存在，则抛出异常，不存在才能新增
        if(bindingMap.get(binding.getQueueName()) != null) {
            throw new MqException("[MemoryDataCenter] 该绑定已经存在. exchangeName="
                    + binding.getExchangeName() +" queueName="+binding.getQueueName());
        }

        bindingMap.put(binding.getQueueName(),binding);
        System.out.println("[MemoryDataCenter] 添加绑定成功. exchangeName="+binding.getExchangeName()
                +",queueName="+binding.getQueueName());
    }

    //获取特定的binding
    public Binding getBinding(String exchangeName,String queueName) {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(exchangeName);
        if(bindingMap == null) {
            return null;
        }
        return bindingMap.get(queueName);
    }
    //获取所有的binding
    public ConcurrentHashMap<String,Binding> getAllBinding(String exchangeName) throws MqException {
        return bindingsMap.get(exchangeName);
    }

    public void deleteBinding(Binding binding) throws MqException {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
        if(bindingMap == null) {
            throw new MqException("[MemoryDataCenter] 不存在的绑定. exchange="+binding.getExchangeName()
            +",queueName="+binding.getQueueName());
        }
        bindingMap.remove(binding.getQueueName());
        System.out.println("[MemoryDataCenter] 删除绑定成功. exchangeName="+binding.getExchangeName()+",queueName="+binding.getQueueName());
    }

    //消息操作
    public void addMessage(Message message) {
        messageMap.put(message.getMessageId(),message);
        System.out.println("[MemoryDataCenter] 添加消息成功. messageId="+message.getMessageId());
    }
    public Message getMessage(String messageId) {
        return messageMap.get(messageId);
    }
    public void deleteMessage(String messageId) {
        messageMap.remove(messageId);
    }
    public ConcurrentHashMap<String,Message> getWaitAckMessage(String queueName) {
        return messageWaitAckMap.get(queueName);
    }
    public void deleteWaitAckMessage(String queueName,String messageId) {
        //先找到对应队列的未确认消息链表
        ConcurrentHashMap<String, Message> waitAckMap = messageWaitAckMap.get(queueName);
        waitAckMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 删除消息成功. messageId="+messageId);
    }

    //发送消息到指定队列
    public void sendMessage(MSGQueue queue,Message message) {
//        LinkedList<Message> messages = queueMessageMap.get(queue.getName());
//        if(messages == null) {
//            messages = new LinkedList<>();
//            queueMessageMap.put(queue.getName(),messages);
//        }
        LinkedList<Message> messages = queueMessageMap.computeIfAbsent(queue.getName(),k -> new LinkedList<>());
        synchronized (messages) {
            messages.add(message);
        }
        //把要插入的消息也向 总的消息中心 存一份
        addMessage(message);

        System.out.println("[MemoryDataCenter] 发送消息到"+queue.getName()+"队列成功"+",queueName="+queue.getName()
                +"messageId="+message.getMessageId());
    }

    //从队列中获取消息,取队列中的第一个元素
    public Message pollMessage(String queueName) {
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null) {
            return null;
        }
        synchronized (messages) {
            if(messages.size() == 0) {
                return null;
            }
            //链表中有值，就进行头删
            Message currentMessage = messages.remove(0);
            System.out.println("[MemoryDataCenter] 取出消息成功. queueName="+queueName+",currentMessageId="+currentMessage.getMessageId());
            return currentMessage;
        }
    }
    //从指定队列中获取所有的消息
    public LinkedList<Message> getAllMessages(MSGQueue queue) {
        return queueMessageMap.get(queue.getName());
    }
    //获取指定队列中的消息个数
    public int getMessageCount(MSGQueue queue) {
        LinkedList<Message> messages = queueMessageMap.get(queue.getName());
        if(messages == null) {
            return 0;
        }
        synchronized (messages) {
            return messages.size();
        }
    }
    //添加未被确认的消息
    public void addWaitAckMessage(MSGQueue queue,Message message) {
        ConcurrentHashMap<String, Message> waitAckMessages = messageWaitAckMap.computeIfAbsent(queue.getName(), k -> new ConcurrentHashMap<>());
        waitAckMessages.put(message.getMessageId(),message);
        System.out.println("[MemoryDataCenter] 添加未确认消息成功. queueName="+queue.getName()+",messageId="+message.getMessageId());
    }

    //删除未确认的消息(消息确认应答)
    public void pollWaitAckMessage(MSGQueue queue,Message message) {
        ConcurrentHashMap<String, Message> waitAckMessages = messageWaitAckMap.get(queue.getName());
        if(waitAckMessages == null) {
            return;
        }
        waitAckMessages.remove(message.getMessageId());
        System.out.println("[MemoryDataCenter] 删除未确认消息成功. queueName="+queue.getName()+",messageId="+message.getMessageId());

    }

    //获取指定的未确认消息
    public Message getWaitAckMessage(MSGQueue queue,String messageId) {
        ConcurrentHashMap<String, Message> waitAckMessages = messageWaitAckMap.get(queue.getName());
        if(waitAckMessages == null) {
            return null;
        }
        return waitAckMessages.get(messageId);
    }

    //从硬盘上加载所有持久化数据到内存,重启时调用此方法，从硬盘中获取数据到内存
    public void recovery(DiskDataCenter diskDataCenter) throws IOException, ClassNotFoundException, MqException {
        //加载数据有：交换机、队列、绑定、消息
        //加载数据之前清空原有数据
        exchangeMap.clear();
        queueMap.clear();
        bindingsMap.clear();
        queueMessageMap.clear();
        messageMap.clear();

        diskDataCenter.init();
        //加载交换机数据
        List<Exchange> exchanges = diskDataCenter.selectAllExchange();
        //把硬盘上的交换机数据加载到内存exchangeMap
        for (Exchange exchange : exchanges) {
            exchangeMap.put(exchange.getName(),exchange);
        }

        //加载队列数据
        List<MSGQueue> queues = diskDataCenter.selectAllQueue();
        for (MSGQueue queue:queues) {
            queueMap.put(queue.getName(),queue);
        }

        //加载绑定数据
        List<Binding> bindings = diskDataCenter.selectAllBinding();
        for (Binding binding:bindings) {
            ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
            bindingMap.put(binding.getQueueName(),binding);
        }
        //加载队列消息数据
        for (Map.Entry<String, MSGQueue> entry: queueMap.entrySet()) {
            LinkedList<Message> messages = diskDataCenter.loadAllMessageFromQueue(entry.getKey());
            queueMessageMap.put(entry.getKey(),messages);
            for (Message message:messages) {
                messageMap.put(message.getMessageId(),message);
            }
        }

        //注意，这里针对 “未被确认的消息” 不需要从硬盘中恢复，当初设定时硬盘也没有这样的结构存放未被确认的消息
        //当服务器重启，服务器与客户端断开联系，未被确认的消息 会被当做 未取走 的消息
        //客户端只需再次从队列中取消息
    }

}
