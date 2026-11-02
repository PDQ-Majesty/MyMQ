package com.pdq.mq.mqserver;

import com.pdq.mq.common.Consumer;
import com.pdq.mq.common.ConsumerEnv;
import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.core.*;
import com.pdq.mq.mqserver.dao.ConsumerManager;
import com.pdq.mq.mqserver.dao.DiskDataCenter;
import com.pdq.mq.mqserver.dao.MemoryDataCenter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description: 这个类表示虚拟主机
 * 包含 交换机、队列、绑定、消息、数据
 * @Auther: pdq
 */
public class VirtualHost {
    //虚拟主机要有交换机、队列、绑定
    private String virtualHostName;
    private DiskDataCenter diskDataCenter = new DiskDataCenter();
    private MemoryDataCenter memoryDataCenter = new MemoryDataCenter();
    //操作交换机的锁对象
    private Object exchangeLocker = new Object();
    //操作队列的锁对象
    private Object queueLocker = new Object();
    private Router router = new Router();

    private ConsumerManager consumerManager = new ConsumerManager(this);

    //构造方法
    public VirtualHost(String virtualHostName) {
        this.virtualHostName = virtualHostName;
        //此时不需要针对MemoryDataCenter进行初始化
        //初始化硬盘数据. 建库建表
        diskDataCenter.init();
        //硬盘数据加载到内存
        try {
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | MqException | ClassNotFoundException e) {
            System.out.println("[VirtualHost] 加载内存失败! virtualHostName=" + virtualHostName);
        }
    }

    public DiskDataCenter getDiskDataCenter() {
        return diskDataCenter;
    }

    public MemoryDataCenter getMemoryDataCenter() {
        return memoryDataCenter;
    }

    //创建交换机,不存在就创建
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType, boolean durable
            , boolean autoDelete, Map<String, Object> arguments) {
        exchangeName = virtualHostName + exchangeName;

        synchronized (exchangeLocker) {
            try {
                //检查交换机是否存在
                if (memoryDataCenter.getExchange(exchangeName) != null) {
                    System.out.println("[VirtualHost] 交换机已经存在!");
                    return true;
                }
                //交换机不存在,创建交换机
                Exchange exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setExchangeType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);

                //如果交换机是持久化存储，就写入硬盘
                if (durable) {
                    diskDataCenter.insertExchange(exchange);
                }
                //把交换机写入内存
                memoryDataCenter.insertExchange(exchange);
                System.out.println("[VirtualHost] 创建交换机成功!");
                return true;
            } catch (Exception e) {
                System.out.println("[VirtualHost] 创建交换机失败! exchangeName=" + exchangeName);
                e.printStackTrace();
                return false;
            }
        }

    }

    //删除交换机
    public boolean exchangeDelete(String exchangeName) {
        exchangeName = virtualHostName + exchangeName;
        synchronized (exchangeLocker) {
            try {
                //从内存中查找该交换机
                Exchange toDelete = memoryDataCenter.getExchange(exchangeName);
                if (toDelete == null) {
                    throw new MqException("[VirtualHost] 该交换机不存在! exchangeName=" + exchangeName);
                }
                if (toDelete.getDurable()) {
                    //从硬盘删除
                    diskDataCenter.deleteExchange(exchangeName);
                }
                //从内存删除
                memoryDataCenter.deleteExchange(exchangeName);
                System.out.println("[VirtualHost] 删除交换机成功!");
                return true;
            } catch (Exception e) {
                System.out.println("[VirtualHost] 删除交换机失败! exchangeName=" + exchangeName);
                e.printStackTrace();
                return false;
            }
        }
    }

    //创建队列
    public boolean queueDeclare(String queueName, boolean durable, boolean autoDelete, boolean exclusive, Map<String, Object> arguments) {
        queueName = virtualHostName + queueName;
        synchronized (queueLocker) {
            try {
                //从内存中查找该队列是否存在
                if (memoryDataCenter.getQueue(queueName) != null) {
                    System.out.println("[VirtualHost] 该队列已经存在! queueName=" + queueName);
                    return true;
                }
                //不存在就创建
                MSGQueue queue = new MSGQueue();
                queue.setName(queueName);
                queue.setDurable(durable);
                queue.setAutoDelete(autoDelete);
                queue.setExclusive(exclusive);
                queue.setArguments(arguments);

                //如果持久化就写入硬盘
                if (durable) {
                    diskDataCenter.insertQueue(queue);
                }
                //写入内存
                memoryDataCenter.insertQueue(queue);
                System.out.println("[VirtualHost] 创建队列成功!");
                return true;
            } catch (Exception e) {
                System.out.println("[VirtualHost] 创建队列失败! queueName=" + queueName);
                e.printStackTrace();
                return false;
            }
        }

    }
    //删除队列

    public boolean queueDelete(String queueName) {
        queueName = virtualHostName + queueName;

        synchronized (queueLocker) {
            try {
                MSGQueue toDelete = memoryDataCenter.getQueue(queueName);
                if (toDelete == null) {
                    throw new MqException("[VirtualHost] 该队列不存在! queueName=" + queueName);
                }
                //硬盘上删除
                if (toDelete.getDurable()) {
                    diskDataCenter.deleteQueue(queueName);
                }
                //内存中删除
                memoryDataCenter.deleteQueue(queueName);
                System.out.println("[VirtualHost] 删除队列成功!");
                return true;
            } catch (Exception e) {
                System.out.println("[VirtualHost] 删除队列失败! queueName=" + queueName);
                e.printStackTrace();
                return false;
            }
        }

    }

    //创建绑定
    public boolean queueBind(String exchangeName, String queueName, String bindingKey) {
        exchangeName = virtualHostName + exchangeName;
        queueName = virtualHostName + queueName;

        synchronized (exchangeLocker) {
            synchronized (queueLocker) {
                try {
                    //判断bindingKey是否合法
                    if (!router.checkBindingKey(bindingKey)) {
                        throw new MqException("[VirtualHost] bindingKey不合法！bindingKey=" + bindingKey);
                    }
                    //查询相应的交换机和队列是否存在
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    if (exchange == null || queue == null) {
                        throw new MqException("[VirtualHost] 交换机或队列不存在! exchangeName=" + exchangeName + ",queueName=" + queueName);
                    }

                    //查找该绑定是否存在
                    if (memoryDataCenter.getBinding(exchangeName, queueName) != null) {
                        System.out.println("[VirtualHost] 绑定已经存在! exchangeName=" + exchangeName
                                + ",queueName=" + queueName);
                        return true;
                    }

                    //不存在就创建
                    Binding binding = new Binding();
                    binding.setExchangeName(exchangeName);
                    binding.setQueueName(queueName);
                    binding.setBindingKey(bindingKey);
                    //如果交换机和队列持久化，绑定也就持久化,写入硬盘
                    if (exchange.getDurable() || queue.getDurable()) {
                        diskDataCenter.insertBinding(binding);
                    }
                    //写入内存
                    memoryDataCenter.insertBinding(binding);
                    System.out.println("[VirtualHost] 创建绑定成功!");
                    return true;
                } catch (Exception e) {
                    System.out.println("[VirtualHost] 创建绑定失败! exchangeName=" + exchangeName + ",queueName=" + queueName);
                    e.printStackTrace();
                    return false;
                }
            }
        }

    }

    //删除绑定
    public boolean queueUnbind(String exchangeName, String queueName) {
        exchangeName = virtualHostName + exchangeName;
        queueName = virtualHostName + queueName;
        synchronized (exchangeLocker) {
            synchronized ((queueLocker)) {
                try {
                    //这里不对交换机和队列的存在性，只对绑定进行校验.
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    Binding toDelete = memoryDataCenter.getBinding(exchangeName, queueName);
                    //判断该绑定是否存在
                    if (toDelete == null) {
                        throw new MqException("[VirtualHost] 绑定不存在! exchangeName=" + exchangeName + ",queueName=" + queueName);
                    }
                    if (exchange.getDurable() || queue.getDurable()) {
                        //硬盘上解除绑定
                        diskDataCenter.deleteBinding(toDelete);
                    }
                    //内存上解除绑定
                    memoryDataCenter.deleteBinding(toDelete);
                    System.out.println("[VirtualHost] 绑定删除成功! ");
                    return true;
                } catch (Exception e) {
                    System.out.println("[VirtualHost] 绑定删除失败! exchangeName=" + exchangeName + ",queueName=" + queueName);
                    e.printStackTrace();
                    return false;
                }
            }
        }

    }

    //发送消息到指定队列
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties, byte[] body) {
        //针对不同交换机类型做出不同的转发操作
        //DIRECT交换机：以Message的RoutingKey作为队列名，将消息转发到队列名为routingKey的队列中
        //fanOut交换机：根据交换机查出该交换机所有的绑定，给所有绑定的队列发送消息
        //Topic交换机：根据交换机查出该交换机所有的绑定，只要有绑定的bindingKey 和 routingKey 一致就发送消息到该队列
        try {
            //转换交换机的名字
            exchangeName = virtualHostName + exchangeName;
            //检查routingKey是否合法
            if (!router.checkRoutingKey(routingKey)) {
                throw new MqException("[VirtualHost] routingKey不合法！routingKey=" + routingKey);
            }
            //先判断该交换机是否存在，后判断交换机类型
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if (exchange == null) {
                throw new MqException("[basicPublish] 交换机不存在! exchangeName=" + exchangeName);
            }
            ExchangeType exchangeType = exchange.getExchangeType();
            if (exchangeType == ExchangeType.DIRECT) {
                //直接交换机,将消息发送到队列名为routingKey的队列中
                //队列在内存中的key带了virtualHostName前缀,查找必须拼接前缀,且判空和取用必须是同一个对象
                String queueName = virtualHostName + routingKey;
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if (queue == null) {
                    throw new MqException("[basicPublish] 要转发消息的队列不存在! queueName=" + queueName);
                }
                //创建消息
                Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                //必须复用上面判过空的 queue 对象,不能再用裸的 routingKey 重新查询
                sendMessage(queue, message);
                //DIRECT投递完毕必须返回,避免落入其他交换机逻辑造成重复投递
                return true;

            } else {
                //扇出交换机 或 主题交换机
                //查找该交换机的绑定
                ConcurrentHashMap<String, Binding> bindingsMap = memoryDataCenter.getAllBinding(exchangeName);
                if(bindingsMap == null || bindingsMap.isEmpty()) {
                    System.out.println("[VirtualHost] basicPublish 交换机没有绑定! exchangeName=" + exchangeName);
                    return true;
                }
                for (Map.Entry<String, Binding> entry : bindingsMap.entrySet()) {
                    // 1) 获取到绑定对象, 判定对应的队列是否存在
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(binding.getQueueName());
                    if (queue == null) {
                        // 此处就不抛出异常了. 可能此处有多个这样的队列.
                        // 希望不要因为一个队列的失败, 影响到其他队列的消息的传输.
                        System.out.println("[VirtualHost] basicPublish 发送消息时, 发现队列不存在! queueName=" + binding.getQueueName());
                        continue;
                    }
                    // 2) 构造消息对象
                    Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                    // 3) 判定这个消息是否能转发给该队列.
                    //    如果是 fanout, 所有绑定的队列都要转发的.
                    //    如果是 topic, 还需要判定下, bindingKey 和 routingKey 是不是匹配.
                    if (!router.route(exchange.getExchangeType(), binding, message)) {
                        continue;
                    }
                    // 4) 真正转发消息给队列
                    sendMessage(queue, message);
                }
            }

            return true;
        } catch (Exception e) {
            System.out.println("[basicPublish] 消息发送失败!");
            e.printStackTrace();
            return false;
        }
    }

    private void sendMessage(MSGQueue queue, Message message) throws InterruptedException {
        //查看是否持久化，存入硬盘
        //1表示不持久化，2表示持久化
        if (message.getDeliverMode() == 2) {
            //持久化存储
            try {
                diskDataCenter.sendMessage(queue, message);
            } catch (IOException | MqException e) {
                System.out.println("[VirtualHost] 消息存储到硬盘失败! queueName=" + queue.getName()
                        + ",messageId=" + message.getMessageId());
            }
        }
        //内存存储
        memoryDataCenter.sendMessage(queue, message);
        //TODO 通知消费者可以消费消息了
        consumerManager.notifyConsume(queue);
    }

    //订阅消息
    // 添加一个队列的订阅者, 当队列收到消息之后, 就要把消息推送给对应的订阅者.
    // consumerTag: 消费者的身份标识
    // autoAck: 消息被消费完成后, 应答的方式. 为 true 自动应答. 为 false 手动应答.
    // consumer: 是一个回调函数. 此处类型设定成函数式接口. 这样后续调用 basicConsume 并且传实参的时候, 就可以写作 lambda 样子
    public boolean basicConsume(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        queueName = virtualHostName + queueName;
        //创建一个ConsumerEnv对象，把这个对象添加到对应的队列中
        try {
            consumerManager.addConsumer(consumerTag, queueName, autoAck, consumer);
            System.out.println("[VirtualHost] 添加订阅者成功！");
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 添加订阅者失败！consumerTag=" + consumerTag + ",queueName=" + queueName);
            e.printStackTrace();
            return false;
        }

    }

    //手动消息应答
    public boolean basicAck(String queueName, String messageId) {
        queueName = virtualHostName + queueName;
        MSGQueue queue = memoryDataCenter.getQueue(queueName);
        Message message = memoryDataCenter.getMessage(messageId);
        try {
            if (queue == null) {
                throw new MqException("[VirtualHost] 消息所在队列不存在! queueName=" + queueName + ",messageId=" + messageId);
            }
            if (message == null) {
                throw new MqException("[VirtualHost] 确认应答的消息不存在! queueName=" + queueName + ",messageId=" + messageId);
            }

            //删除硬盘上的消息
            if (message.getDeliverMode() == 2) {
                diskDataCenter.deleteMessage(queue, message);
            }
            //删除内存中的消息
            memoryDataCenter.deleteMessage(message.getMessageId());
            //删除待确认消息队列中的消息
            memoryDataCenter.deleteWaitAckMessage(queue.getName(), message.getMessageId());
            System.out.println("[VirtualHost] 手动确认消息应答成功!");
            return true;
        } catch (Exception e) {
            System.out.println("[VirtualHost] 手动确认应答失败! queueName=" + queueName + ",messageId=" + messageId);
            e.printStackTrace();
        }
        return false;
    }
}
