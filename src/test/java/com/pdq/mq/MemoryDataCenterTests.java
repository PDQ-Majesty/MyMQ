package com.pdq.mq;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.core.*;
import com.pdq.mq.mqserver.dao.DiskDataCenter;
import com.pdq.mq.mqserver.dao.MemoryDataCenter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description:
 * @Auther: pdq
 */
@SpringBootTest
public class MemoryDataCenterTests {
    private MemoryDataCenter memoryDataCenter = null;
    private DiskDataCenter diskDataCenter = null;
    //测试准备工作
    @BeforeEach
    public void setUp() {
        memoryDataCenter = new MemoryDataCenter();
        diskDataCenter = new DiskDataCenter();
    }

    //测试收尾工作
    @AfterEach
    public void tearDown() {
        memoryDataCenter = null;
        diskDataCenter = null;
    }

    //创建测试交换机
    private Exchange createExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setExchangeType(ExchangeType.DIRECT);
        exchange.setName(exchangeName);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        return exchange;
    }

    //创建测试队列
    private MSGQueue createQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setExclusive(false);
        queue.setAutoDelete(false);
        return queue;
    }

    //创建测试绑定
    private Binding createBinding(String exchangeName,String queueName,String bindingKey) {
        Binding binding = new Binding();
        Exchange exchange = createExchange(exchangeName);
        MSGQueue queue = createQueue(queueName);
        binding.setExchangeName(exchange.getName());
        binding.setQueueName(queue.getName());
        binding.setBindingKey(bindingKey);
        return binding;
    }

    //创建测试消息
    private Message createMessage(String content) {
        Message message = Message.createMessageWithId("testRoutingKey",null, content.getBytes());
        return message;
    }

    //测试交换机操作
    @Test
    public void exchangeTest() {
        Exchange expectedExchange = createExchange("testExchange");
        memoryDataCenter.insertExchange(expectedExchange);
        Exchange actualExchange = memoryDataCenter.getExchange(expectedExchange.getName());
        Assertions.assertEquals(expectedExchange,actualExchange);
        Assertions.assertEquals("testExchange",actualExchange.getName());
        Assertions.assertEquals(ExchangeType.DIRECT,actualExchange.getExchangeType());
        Assertions.assertEquals(true,actualExchange.getDurable());
        Assertions.assertEquals(false,actualExchange.getAutoDelete());

        memoryDataCenter.deleteExchange(expectedExchange.getName());
        actualExchange = memoryDataCenter.getExchange(expectedExchange.getName());
        Assertions.assertNull(actualExchange);
    }

    //测试队列操作
    @Test
    public void queueTest() {
        MSGQueue expectedQueue = createQueue("testQueue");
        memoryDataCenter.insertQueue(expectedQueue);
        MSGQueue actualQueue = memoryDataCenter.getQueue(expectedQueue.getName());
        Assertions.assertEquals(expectedQueue,actualQueue);
        Assertions.assertEquals("testQueue",actualQueue.getName());
        Assertions.assertEquals(true,actualQueue.getDurable());
        Assertions.assertEquals(false,actualQueue.getExclusive());
        Assertions.assertEquals(false,actualQueue.getAutoDelete());

        memoryDataCenter.deleteQueue(expectedQueue.getName());
        actualQueue = memoryDataCenter.getQueue(expectedQueue.getName());
        Assertions.assertNull(actualQueue);
    }

    //测试绑定操作
    @Test
    public void bindingTest() throws MqException {
        Binding expectedBinding = createBinding("testExchange","testQueue","testBindingKey");
        memoryDataCenter.insertBinding(expectedBinding);
        Binding actualBinding = memoryDataCenter.getBinding(expectedBinding.getExchangeName(), expectedBinding.getQueueName());
        Assertions.assertEquals(expectedBinding,actualBinding);
        Assertions.assertEquals("testExchange",actualBinding.getExchangeName());
        Assertions.assertEquals("testQueue",actualBinding.getQueueName());
        Assertions.assertEquals("testBindingKey",actualBinding.getBindingKey());

        ConcurrentHashMap<String, Binding> bindingMap = memoryDataCenter.getAllBinding("testExchange");
        Assertions.assertEquals(1,bindingMap.size());
        Assertions.assertEquals(expectedBinding,bindingMap.get("testQueue"));

        memoryDataCenter.deleteBinding(expectedBinding);
        actualBinding = memoryDataCenter.getBinding(expectedBinding.getExchangeName(), expectedBinding.getQueueName());
        Assertions.assertNull(actualBinding);
    }

    //测试消息操作
    @Test
    public void messageTest() {
        Message expectedMessage = createMessage("testMessage");
        memoryDataCenter.addMessage(expectedMessage);
        Message actualMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage,actualMessage);

        //发送消息到指定队列
        MSGQueue queue = createQueue("testQueue");
        memoryDataCenter.sendMessage(queue,expectedMessage);
        LinkedList<Message> messages = memoryDataCenter.getAllMessages(queue);
        Assertions.assertEquals(1,messages.size());
        //取出消息
        actualMessage = memoryDataCenter.pollMessage("testQueue");
        Assertions.assertEquals(expectedMessage,actualMessage);

        memoryDataCenter.deleteMessage(expectedMessage.getMessageId());
        actualMessage = memoryDataCenter.getMessage(expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);

    }

    //测试发送消息到指定队列
    @Test
    public void sendMessageTest() {
        MSGQueue queue = createQueue("testQueue");
        List<Message> expectedMessages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = createMessage("testMessage" + i);
            expectedMessages.add(message);
            memoryDataCenter.sendMessage(queue,message);
        }

        List<Message> actualMessages = new ArrayList<>();
        while(true) {
            Message message = memoryDataCenter.pollMessage("testQueue");
            if(message == null) {
                break;
            }
            actualMessages.add(message);
        }

        for (int i = 0; i < 10; i++) {
            Assertions.assertEquals(expectedMessages.get(i),actualMessages.get(i));
        }
    }

    //测试未被确认消息操作
    @Test
    public void waitAckMessageTest() {

        MSGQueue waitAckAQueue = createQueue("waitAckMessageQueue");
        Message expectedMessage = createMessage("waitAckMessage");
        memoryDataCenter.addWaitAckMessage(waitAckAQueue,expectedMessage);
        ConcurrentHashMap<String, Message> waitAckMap = memoryDataCenter.getWaitAckMessage(waitAckAQueue.getName());
        Message actualMessage = waitAckMap.get(expectedMessage.getMessageId());
        Assertions.assertEquals(expectedMessage,actualMessage);

        memoryDataCenter.deleteWaitAckMessage("waitAckMessageQueue", expectedMessage.getMessageId());
        actualMessage = waitAckMap.get(expectedMessage.getMessageId());
        Assertions.assertNull(actualMessage);
    }

    //测试加载硬盘数据到内存操作
    @Test
    public void recoveryTest() throws IOException, MqException, ClassNotFoundException {
        //【改造说明】原实现在用例内 SpringApplication.run 覆盖并关闭静态 MqApplication.context,
        // 最后 FileUtils.deleteDirectory 整个 ./data —— 前者会击穿其他测试类的上下文(顺序耦合),
        // 后者会连开发/其他用例的持久化数据一起删除, 且 Windows 句柄未释放时直接报错
        // (曾实测 "Unable to delete file: .\data\meta.db"). 现在上下文由 TestHarnessBase 统一管理,
        // 清理只针对本用例创建的资源.
        diskDataCenter.init();
        //首先硬盘上创建数据(随机唯一名, 避免与开发服务器/demo 留下的 durable 数据撞名)
        String exchangeName = "testExchange" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String queueName = "testQueue" + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        Exchange expectedExchange = createExchange(exchangeName);
        MSGQueue expectedQueue = createQueue(queueName);
        Binding expectedBinding = createBinding(expectedExchange.getName(), expectedQueue.getName(), "testBindingKey");
        Message expectedMessage = createMessage("testMessage");

        diskDataCenter.insertExchange(expectedExchange);
        diskDataCenter.insertQueue(expectedQueue);
        diskDataCenter.insertBinding(expectedBinding);
        diskDataCenter.sendMessage(expectedQueue,expectedMessage);

        //调用recovery()方法，恢复数据到内存
        memoryDataCenter.recovery(diskDataCenter);
        //从内存中取出数据，比较 该数据 与 硬盘上准备的数据 是否相同
        Exchange actualExchange = memoryDataCenter.getExchange(expectedExchange.getName());
        MSGQueue actualQueue = memoryDataCenter.getQueue(expectedQueue.getName());
        Binding actualBinding = memoryDataCenter.getBinding(expectedExchange.getName(), expectedQueue.getName());
        Message actualMessage = memoryDataCenter.pollMessage(expectedQueue.getName());
        Assertions.assertNotNull(actualMessage, "recovery后应能从队列中取出持久化消息");

        Assertions.assertEquals(expectedExchange.getName(),actualExchange.getName());
        Assertions.assertEquals(expectedExchange.getExchangeType(),actualExchange.getExchangeType());
        Assertions.assertEquals(expectedExchange.getDurable(),actualExchange.getDurable());
        Assertions.assertEquals(expectedExchange.getAutoDelete(),actualExchange.getAutoDelete());

        Assertions.assertEquals(expectedQueue.getName(),actualQueue.getName());
        Assertions.assertEquals(expectedQueue.getDurable(),actualQueue.getDurable());
        Assertions.assertEquals(expectedQueue.getAutoDelete(),actualQueue.getAutoDelete());
        Assertions.assertEquals(expectedQueue.getExclusive(),actualQueue.getExclusive());

        Assertions.assertEquals(expectedBinding.getExchangeName(),actualBinding.getExchangeName());
        Assertions.assertEquals(expectedBinding.getQueueName(),actualBinding.getQueueName());
        Assertions.assertEquals(expectedBinding.getBindingKey(),actualBinding.getBindingKey());

        Assertions.assertEquals(expectedMessage.getMessageId(),actualMessage.getMessageId());
        Assertions.assertArrayEquals(expectedMessage.getBody(),actualMessage.getBody());
        Assertions.assertEquals(expectedMessage.getRoutingKey(),actualMessage.getRoutingKey());


        //清理测试过程中在硬盘上产生的数据(只清理本用例创建的资源, 不动别人的数据)
        diskDataCenter.deleteBinding(expectedBinding);
        diskDataCenter.deleteQueue(expectedQueue.getName());
        diskDataCenter.deleteExchange(expectedExchange.getName());

    }
}
