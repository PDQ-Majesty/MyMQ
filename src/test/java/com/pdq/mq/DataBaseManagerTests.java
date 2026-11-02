package com.pdq.mq;

import com.pdq.mq.mqserver.core.Binding;
import com.pdq.mq.mqserver.core.Exchange;
import com.pdq.mq.mqserver.core.ExchangeType;
import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.dao.DataBaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;

/**
 * @Description: 单元测试DataBaseManager类中针对数据库操作的方法
 * @Auther: pdq
 */
@SpringBootTest
public class DataBaseManagerTests {
    private DataBaseManager dataBaseManager = new DataBaseManager();

    //【改造说明】原实现每个用例都 start/close 一次 Spring 上下文并删库:
    //  1. 固定端口 8080 会与其他 @SpringBootTest 用例冲突;
    //  2. Windows 下文件句柄释放有延迟, 偶发删库失败 -> 脏数据残留跨用例累积,
    //     "size()==2" 这类绝对行数/下标断言随执行顺序漂移(曾实测 expected 2 but was 3).
    // 现在上下文生命周期收敛到 TestHarnessBase 统一管理; 所有测试资源使用随机唯一名,
    // 与开发服务器留下的业务数据(如 demo 创建的 durable testQueue/testExchange)彻底隔离,
    // 断言"按名字查找", 不依赖绝对行数/下标/全局 bindings 清空假设.
    @BeforeEach
    public void setup() {
        dataBaseManager.init();
    }

    private static String uniqueName(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    //对初始化数据库中插入的默认数据进行单元测试，
    @Test
    public void initTest() {
        //初始库中应存在且仅校验"默认交换机"这一契约, 不做全局行数假设(本地共享库可能有业务数据)
        Exchange defaultExchange = findExchange("");
        Assertions.assertNotNull(defaultExchange, "应存在默认交换机");
        Assertions.assertEquals(1, dataBaseManager.selectAllExchange().stream()
                .filter(e -> "".equals(e.getName())).count(), "默认交换机应唯一");
        Assertions.assertEquals(ExchangeType.DIRECT,defaultExchange.getExchangeType());
        Assertions.assertEquals(true,defaultExchange.getDurable());
        Assertions.assertEquals(false,defaultExchange.getAutoDelete());
    }

    private Exchange createExchange(String exchangeName) {
        Exchange exchange = new Exchange();
        exchange.setName(exchangeName);
        exchange.setExchangeType(ExchangeType.FANOUT);
        exchange.setAutoDelete(false);
        exchange.setDurable(true);
        exchange.setArguments("aaa","111");
        return exchange;
    }
    private Exchange findExchange(String name) {
        return dataBaseManager.selectAllExchange().stream()
                .filter(e -> name.equals(e.getName())).findFirst().orElse(null);
    }

    //测试插入exchange到数据库
    @Test
    public void exchangeInsertTest() {
        String name = uniqueName("newExchange");
        Exchange newExchange = createExchange(name);
        dataBaseManager.insertExchange(newExchange);
        //按名字断言存在性与字段值, 不依赖绝对行数/下标(避免脏数据导致的顺序敏感)
        Exchange stored = findExchange(name);
        Assertions.assertNotNull(stored, "插入后应能按名字查到交换机");
        Assertions.assertEquals(ExchangeType.FANOUT, stored.getExchangeType());
        Assertions.assertEquals(false, stored.getAutoDelete());
        Assertions.assertEquals(true, stored.getDurable());
        Assertions.assertEquals("111", stored.getArguments("aaa"));
        dataBaseManager.deleteExchange(name);
    }
    //测试删除exchange
    @Test
    public void exchangeDeleteTest() {
        //先构造一个交换机,插入数据库，确认存在后删除, 再确认消失
        String name = uniqueName("testExchange");
        Exchange exchange = createExchange(name);
        dataBaseManager.insertExchange(exchange);
        Assertions.assertNotNull(findExchange(name));
        //删除交换机
        dataBaseManager.deleteExchange(name);
        Assertions.assertNull(findExchange(name), "删除后不应再查到该交换机");
        //默认交换机不受影响
        Assertions.assertNotNull(findExchange(""), "删除操作不应误伤默认交换机");
    }

    private MSGQueue createQueue(String queueName) {
        MSGQueue queue = new MSGQueue();
        queue.setName(queueName);
        queue.setDurable(true);
        queue.setAutoDelete(false);
        queue.setExclusive(false);
        queue.setArguments("aaa","111");
        return queue;
    }
    private MSGQueue findQueue(String name) {
        return dataBaseManager.selectAllQueue().stream()
                .filter(q -> name.equals(q.getName())).findFirst().orElse(null);
    }

    //测试插入queue
    @Test
    public void queueInsertTest() {
        //先创建一个MSGQueue并插入
        String name = uniqueName("testQueue");
        MSGQueue newQueue = createQueue(name);
        dataBaseManager.insertQueue(newQueue);
        MSGQueue stored = findQueue(name);
        Assertions.assertNotNull(stored, "插入后应能按名字查到队列");
        Assertions.assertEquals(false, stored.getAutoDelete());
        Assertions.assertEquals(true, stored.getDurable());
        Assertions.assertEquals(false, stored.getExclusive());
        Assertions.assertEquals("111", stored.getArguments("aaa"));
        dataBaseManager.deleteQueue(name);
    }
    @Test
    public void queueDeleteTest() {
        //先构造一个queue,插入数据库，确认存在后删除, 再确认消失
        String name = uniqueName("testQueue");
        MSGQueue newQueue = createQueue(name);
        dataBaseManager.insertQueue(newQueue);
        Assertions.assertNotNull(findQueue(name));
        //删除queue
        dataBaseManager.deleteQueue(name);
        Assertions.assertNull(findQueue(name), "删除后不应再查到该队列");
    }

    private Binding createBinding(String exchangeName,String queueName,String bindingKey) {
        Binding binding = new Binding();
        binding.setExchangeName(exchangeName);
        binding.setQueueName(queueName);
        binding.setBindingKey(bindingKey);
        return binding;
    }
    private Binding findBinding(String exchangeName, String queueName) {
        return dataBaseManager.selectAllBinding().stream()
                .filter(b -> exchangeName.equals(b.getExchangeName()) && queueName.equals(b.getQueueName()))
                .findFirst().orElse(null);
    }

    @Test
    public void bindingInsertTest() {
        String exchangeName = uniqueName("testExchange");
        String queueName = uniqueName("testQueue");
        Exchange exchange = createExchange(exchangeName);
        MSGQueue queue = createQueue(queueName);
        dataBaseManager.insertExchange(exchange);
        dataBaseManager.insertQueue(queue);
        Binding binding = createBinding(exchange.getName(), queue.getName(),"vvv");
        dataBaseManager.insertBinding(binding);
        //校验预期值
        Binding stored = findBinding(exchangeName, queueName);
        Assertions.assertNotNull(stored, "插入后应能按交换机+队列名查到绑定");
        Assertions.assertEquals("vvv", stored.getBindingKey());
        //收尾, 避免残留
        dataBaseManager.deleteBinding(binding);
        dataBaseManager.deleteQueue(queueName);
        dataBaseManager.deleteExchange(exchangeName);
    }

    @Test
    public void bindingDeleteTest() {
        //还是先创建再删除
        String exchangeName = uniqueName("testExchange");
        String queueName = uniqueName("testQueue");
        Exchange exchange = createExchange(exchangeName);
        MSGQueue queue = createQueue(queueName);
        dataBaseManager.insertExchange(exchange);
        dataBaseManager.insertQueue(queue);
        Binding binding = createBinding(exchange.getName(), queue.getName(),"vvv");
        dataBaseManager.insertBinding(binding);
        Assertions.assertNotNull(findBinding(exchangeName, queueName));
        Binding toDelBinding = createBinding(exchangeName, queueName,"vvv");
        dataBaseManager.deleteBinding(toDelBinding);
        Assertions.assertNull(findBinding(exchangeName, queueName), "删除后不应再查到该绑定");
        //收尾
        dataBaseManager.deleteQueue(queueName);
        dataBaseManager.deleteExchange(exchangeName);
    }
}
