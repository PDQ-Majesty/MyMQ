package com.pdq.mq.mqserver.dao;

import com.pdq.mq.common.Consumer;
import com.pdq.mq.common.ConsumerEnv;
import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.VirtualHost;
import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @Description:
 * @Auther: pdq
 */
public class ConsumerManager {
    //VirtualHost和ConsumerManager相互引用
    private VirtualHost parent;

    //创建一个线程池来实现具体的回调任务
    private ExecutorService workPool = Executors.newFixedThreadPool(4);

    //创建一个阻塞队列来记录有哪些队列接收到了消息(令牌队列)
    private BlockingQueue<String> tokenQueue = new LinkedBlockingQueue<>();
    //创建一个扫描线程来感知阻塞队列
    private Thread scannerThread = null;

    public ConsumerManager(VirtualHost parent) {
        this.parent = parent;

        //创建线程任务
        scannerThread = new Thread(() -> {
            //扫描阻塞队列.获取队列名
            while (true) {
                try {
                    //获取令牌
                    String queueName = tokenQueue.take();
                    //根据令牌找到队列
                    MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
                    if (queue == null) {
                        throw new MqException("[ConsumerManager] 通过令牌找到的队列不存在! queueName=" + queueName);
                    }
                    synchronized (queue) {
                        //消费消息
                        consumerMessage(queue);
                    }
                } catch (InterruptedException | MqException e) {
                    e.printStackTrace();
                }
            }
        });
        //把线程设置成后台线程
        scannerThread.setDaemon(true);
        //启动线程
        scannerThread.start();


    }

    //该方法调用时机是发送消息的时候
    public void notifyConsume(MSGQueue queue) throws InterruptedException {
        //把接收消息的队列名 添加到 阻塞队列中
        tokenQueue.put(queue.getName());
    }

    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer) throws MqException {
        //查找当前到订阅消息的队列，判断是否存在
        MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
        if (queue == null) {
            throw new MqException("[ConsumerManager] 要订阅的队列不存在！queueName=" + queueName);
        }
        //队列存在，添加消费者
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
        synchronized (queue) {
            queue.addConsumer(consumerEnv);
            //存在一种情况,生产者先启动,消费者后启动.队列中已经产生了一些消息,这时需要先把队列中的消息先消费掉
            //这个消费的动作由线程池来完成
            //获取当前队列中有几条消息
            int messageCount = parent.getMemoryDataCenter().getMessageCount(queue);
            for (int i = 0; i < messageCount; i++) {
                //调用一次就消费一个消息, 一共消费 messageCount 个消息
                consumerMessage(queue);
            }
        }
    }

    private void consumerMessage(MSGQueue queue) {
        // 使用轮询的方式进行消费
        ConsumerEnv luckyDog = queue.chooseConsumer(queue);
        if (luckyDog == null) {
            System.out.println("[ConsumerManager] 当前队列没有订阅者! queueName=" + queue.getName());
            //暂时不消费,等队列中有订阅者再说
            return;
        }
        //取出消息, 让此消费者进行消费
        Message message = parent.getMemoryDataCenter().pollMessage(queue.getName());
        //把消息丢到线程池中进行处理,线程池调用回调函数进行处理
        workPool.submit(() -> {
            try {
                //先把要消费的消息存入到 待消费的队列中,防止在消费过程中出现消费失败的情况导致该消息丢失
                parent.getMemoryDataCenter().addWaitAckMessage(queue, message);
                //调用回调函数，消费消息
                luckyDog.getConsumer().handleDelivery(luckyDog.getConsumerTag(), message.getBasicProperties(), message.getBody());
                //校验该消费者是否为自动应答
                if (luckyDog.isAutoAck()) {
                    //自动应答
                    //删除硬盘上的消息
                    if (message.getDeliverMode() == 2) {
                        parent.getDiskDataCenter().deleteMessage(queue, message);
                    }
                    //删除内存中的消息
                    parent.getMemoryDataCenter().deleteMessage(message.getMessageId());
                    //删除待确认消息队列中的消息
                    parent.getMemoryDataCenter().deleteWaitAckMessage(queue.getName(), message.getMessageId());
                }
                System.out.println("[ConsumerManager] 消费者消费消息成功!");
            } catch (MqException | IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
    }
}
