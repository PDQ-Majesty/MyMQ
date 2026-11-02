package com.pdq.mq;

import com.pdq.mq.mqclient.Channel;
import com.pdq.mq.mqclient.Connection;
import com.pdq.mq.mqclient.ConnectionFactory;
import com.pdq.mq.mqserver.BrokerServer;
import com.pdq.mq.mqserver.core.ExchangeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 协议链路回归测试:
 * 通过真实 TCP 链路(客户端 Channel -> BrokerServer -> VirtualHost)逐个校验 type 码请求-响应闭环,
 * 重点覆盖容易被纯单元测试漏掉的"客户端参数类 -> 服务端反序列化强转"这一环.
 * 【已发现的真实缺陷】type=0x8(解除绑定) 服务端把客户端发来的 QueueUnbindArguments
 * 强转成无继承关系的 QueueBindArguments, 必然 ClassCastException; 又因该异常未被
 * processConnection 捕获, 服务端不回响应, 客户端 waitResult() 无超时 -> 永久阻塞.
 * @Auther: pdq
 */
@SpringBootTest
public class BrokerProtocolRegressionTest {

    private BrokerServer broker;
    private int port;
    private Connection connection;

    //注意: 必须用实例级 @BeforeEach 而不是静态 @BeforeAll 启动 BrokerServer.
    //因为 @BeforeAll 早于 Spring TestContext 初始化, 此时 MqApplication.context 仍为 null,
    //而 VirtualHost 构造链 DataBaseManager.init() 依赖该上下文取 MetaMapper Bean.
    @BeforeEach
    public void startBrokerAndConnect() throws IOException {
        //向操作系统申请空闲端口, 避免与本地开发的 9090 冲突
        try (ServerSocket probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        broker = new BrokerServer(port);
        Thread t = new Thread(broker::start, "regression-broker-" + port);
        t.setDaemon(true);
        t.start();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(port);
        connection = factory.newConnection();
    }

    @AfterEach
    public void stopBrokerAndConnect() throws IOException {
        if (connection != null) {
            connection.close();
        }
        if (broker != null) {
            broker.stop();
        }
    }

    /** 生成唯一资源名, 只含字母数字以满足 routingKey/bindingKey 字符约束 */
    private static String uniq(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    /**
     * 客户端 waitResult() 没有超时机制, 一旦服务端异常没回响应就会永久挂住,
     * 因此所有远程调用都必须包在带超时的 Future 里, 把"卡死"转成可断言的失败.
     */
    private boolean callWithTimeout(java.util.function.Supplier<Boolean> action) throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> f = pool.submit(action::get);
            try {
                return f.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                f.cancel(true);
                pool.shutdownNow();
                throw new AssertionError("10秒内未收到服务端响应, 疑似服务端异常导致连接卡死", e);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void queueUnbind_overTcp_shouldSucceed() throws Exception {
        String exchange = uniq("ex");
        String queue = uniq("q");
        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel, "channel 创建失败");

        //用非持久化资源, 避免污染 ./data 目录
        Assertions.assertTrue(channel.exchangeDeclare(exchange, ExchangeType.TOPIC, false, false, null));
        Assertions.assertTrue(channel.queueDeclare(queue, false, false, false, null));
        Assertions.assertTrue(channel.queueBind(queue, exchange, "a.b"));

        //解除绑定: 缺陷版本会在此处因服务端 ClassCastException 而永久阻塞
        boolean unbound = callWithTimeout(() -> {
            try {
                return channel.queueUnbind(queue, exchange);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Assertions.assertTrue(unbound, "queueUnbind 应返回 true");

        channel.close();
    }

    /**
     * 解绑后绑定关系应真正消失: 再发一次消息不应投递到该队列.
     * 这条断言顺带证明 0x7 绑定/0x8 解绑在内存数据层的效果正确.
     */
    @Test
    public void publishAfterUnbind_shouldNotDeliver() throws Exception {
        String exchange = uniq("ex");
        String queue = uniq("q");
        Channel channel = connection.createChannel();
        Assertions.assertTrue(channel.exchangeDeclare(exchange, ExchangeType.TOPIC, false, false, null));
        Assertions.assertTrue(channel.queueDeclare(queue, false, false, false, null));
        Assertions.assertTrue(channel.queueBind(queue, exchange, "a.b"));
        Assertions.assertTrue(callWithTimeout(() -> {
            try {
                return channel.queueUnbind(queue, exchange);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }), "解除绑定应成功");

        java.util.Set<String> got = java.util.concurrent.ConcurrentHashMap.newKeySet();
        Channel consumerChannel = connection.createChannel();
        Assertions.assertTrue(consumerChannel.basicConsume(queue, true,
                (consumerTag, basicProperties, body) -> got.add(new String(body))));

        Assertions.assertTrue(channel.basicPublish(exchange, "a.b", null, "should-drop".getBytes()));
        //留一点时间窗口给可能的错误投递
        Thread.sleep(1500);
        Assertions.assertFalse(got.contains("should-drop"), "已解绑的队列不应再收到消息");

        channel.close();
        consumerChannel.close();
    }
}
