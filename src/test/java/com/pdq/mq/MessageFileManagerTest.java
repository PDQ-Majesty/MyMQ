package com.pdq.mq;

import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.core.Message;
import com.pdq.mq.mqserver.dao.MessageFileManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

/**
 * @Description: 测试MessageFileManager中方法
 * @Auther: pdq
 */
@SpringBootTest
public class MessageFileManagerTest {
    MessageFileManager messageFileManager = new MessageFileManager();
    public static final String queueName1 = "testQueue1";
    public static final String queueName2 = "testQueue2";

    //开始测试前的准备工作
    @BeforeEach
    public void setUp() throws IOException {
        //创建两个队列文件，以备后用
        messageFileManager.createQueueFiles(queueName1);
        messageFileManager.createQueueFiles(queueName2);
    }

    //测试结束后的首尾工作
    @AfterEach
    public void tearDown() {
        //把创建的队列删除
        messageFileManager.destroyQueueFiles(queueName1);
        messageFileManager.destroyQueueFiles(queueName2);
    }
   //测试setUp阶段的创建队列
    @Test
    public void createQueueFilesTest() {
        File dataFile1 = new File("./data/"+queueName1+"/queue_data.txt");
        Assertions.assertEquals(true,dataFile1.isFile());
        File statFile1 = new File("./data/"+queueName1+"/queue_stat.txt");
        Assertions.assertEquals(true,statFile1.isFile());

        File dataFile2 = new File("./data/"+queueName2+"/queue_data.txt");
        Assertions.assertEquals(true,dataFile2.isFile());
        File statFile2 = new File("./data/"+queueName2+"/queue_stat.txt");
        Assertions.assertEquals(true,statFile2.isFile());
    }

    //测试统计文件的读写
    @Test
    public void writeReadStatTest() throws IOException {
        MessageFileManager.Stat testStat = new MessageFileManager.Stat();
        testStat.totalCount = 50;
        testStat.valCount = 30;
        ReflectionTestUtils.invokeMethod(messageFileManager,"writeQueueStat",queueName1,testStat);
        MessageFileManager.Stat realStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readQueueStat", queueName1);
        Assertions.assertEquals(50,realStat.totalCount);
        Assertions.assertEquals(30,realStat.valCount);
    }
    //测试删除队列文件目录
    @Test
    public void destroyQueueFilesTest() {
        messageFileManager.destroyQueueFiles(queueName1);
        File dataFile1 = new File("./data/"+queueName1+"/queue_data.txt");
        File statFile1 = new File("./data/"+queueName1+"/queue_stat.txt");
        Assertions.assertEquals(false,dataFile1.exists());
        Assertions.assertEquals(false,statFile1.exists());
    }

    public MSGQueue createTestQueue() {
        MSGQueue queue = new MSGQueue();
        queue.setName("testQueue1");
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        return queue;
    }

    public Message createMessage(String content) {
        Message message = Message.createMessageWithId("testRoutingKey", null, content.getBytes());
        return message;
    }

    //测试发送消息到队列
    @Test
    public void sendMessageTest() throws IOException, MqException, ClassNotFoundException {

        MSGQueue queue = createTestQueue();
        Message message = createMessage("testMessage");
        messageFileManager.sendMessage(queue,message);
        //测试stat文件
        MessageFileManager.Stat curStat = ReflectionTestUtils.invokeMethod(messageFileManager, "readQueueStat", queue.getName());
        Assertions.assertEquals(1,curStat.totalCount);
        Assertions.assertEquals(1,curStat.valCount);
        //测试data文件
        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        Assertions.assertEquals(1,messages.size());
        Assertions.assertEquals(message.getRoutingKey(),messages.get(0).getRoutingKey());
        Assertions.assertArrayEquals(message.getBody(),messages.get(0).getBody());
    }

    //测试删除消息
    @Test
    public void deleteMessageTest() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue();
        //创建十个消息并写入队列
        for (int i = 0; i < 10; i++) {
            Message message = createMessage("testMessage" + i);
            messageFileManager.sendMessage(queue,message);
        }

        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        Assertions.assertEquals(10,messages.size());
        System.out.println(messages.get(2));
        //删除其中的三个消息
        messageFileManager.deleteMessage(queue,messages.get(2));
        messageFileManager.deleteMessage(queue,messages.get(4));
        messageFileManager.deleteMessage(queue,messages.get(6));
        LinkedList<Message> realMessages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        //检查剩下的消息是否符合预期
        Assertions.assertEquals(7,realMessages.size());
        for (int i = 0; i==0||(i+1)/2==0; i++) {
            Assertions.assertEquals(messages.get(i).getRoutingKey(),realMessages.get(i).getRoutingKey());
            Assertions.assertEquals(messages.get(i).getMessageId(),realMessages.get(i).getMessageId());
            Assertions.assertArrayEquals(messages.get(i).getBody(),realMessages.get(i).getBody());
        }
    }
    //测试从队列中取出消息，loadAllMessageFromQueue
    @Test
    public void loadAllMessageFromQueueTest() throws IOException, MqException, ClassNotFoundException {
        Message message1 = createMessage("testMessage1");
        Message message2 = createMessage("testMessage2");
        Message message3 = createMessage("testMessage3");
        MSGQueue queue = createTestQueue();
        messageFileManager.sendMessage(queue,message1);
        messageFileManager.sendMessage(queue,message2);
        messageFileManager.sendMessage(queue,message3);
        long offSetBeg = message1.getOffsetBeg();
        long offSetEnd = message1.getOffsetEnd();
        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        long realOffsetBeg = messages.get(0).getOffsetBeg();
        long realOffsetEnd = messages.get(0).getOffsetEnd();

        Assertions.assertEquals(3,messages.size());
        Assertions.assertArrayEquals(message1.getBody(),messages.get(0).getBody());
        Assertions.assertArrayEquals(message2.getBody(),messages.get(1).getBody());
        Assertions.assertArrayEquals(message3.getBody(),messages.get(2).getBody());

        messageFileManager.deleteMessage(queue,message2);
        messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        Assertions.assertEquals(2,messages.size());
        Assertions.assertArrayEquals(message1.getBody(),messages.get(0).getBody());
        Assertions.assertArrayEquals(message3.getBody(),messages.get(1).getBody());
    }

    //测试GC垃圾回收
    @Test
    public void gcQueueTest() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue();
        //【修复说明】生产代码 checkGC 的触发条件是 totalCount>=2000 且有效率<50%,
        // 原用例只写100条消息就断言 checkGC==true, 与生产阈值矛盾(测试与实现脱节的典型缺陷,
        // 意味着 GC 真实触发路径从未被覆盖过). 现按生产阈值构造数据.
        final int total = 4000;
        for (int i = 0; i < total; i++) {
            Message message = createMessage("testMessage" + i);
            messageFileManager.sendMessage(queue,message);
        }

        //删除前 2501 条 -> 有效率 1499/4000 < 50%, 且总量 >= 2000, 满足 GC 条件
        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        Assertions.assertEquals(total,messages.size());
        for (int i = 0; i < 2501; i++) {
            messageFileManager.deleteMessage(queue,messages.get(i));
        }
        messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        Assertions.assertEquals(1499,messages.size());

        Assertions.assertEquals(true,messageFileManager.checkGC(queue), "达到生产阈值且有效率<50%时应触发GC");
        messageFileManager.gcQueue(queue);
        messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        Assertions.assertEquals(1499,messages.size());
        //GC 后中间产物 new 文件不应残留
        Assertions.assertEquals(false,new File("./data/"+queue.getName()+"/queue_data_new.txt").exists());
    }

    //测试GC触发条件: 未达阈值时不应触发
    @Test
    public void checkGC_belowThreshold_shouldNotTrigger() throws IOException, MqException, ClassNotFoundException {
        MSGQueue queue = createTestQueue();
        //100条 < 2000 阈值, 即便有效率已经低于50%也不应触发
        for (int i = 0; i < 100; i++) {
            messageFileManager.sendMessage(queue, createMessage("noGcMsg" + i));
        }
        LinkedList<Message> messages = messageFileManager.loadAllMessageFromQueue(queue.getName());
        for (int i = 0; i < 51; i++) {
            messageFileManager.deleteMessage(queue,messages.get(i));
        }
        Assertions.assertEquals(false,messageFileManager.checkGC(queue), "总量未达2000阈值时不应触发GC");
    }


}