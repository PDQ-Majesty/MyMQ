package com.pdq.mq;

import com.pdq.mq.common.MqException;
import com.pdq.mq.mqserver.core.Binding;
import com.pdq.mq.mqserver.core.ExchangeType;
import com.pdq.mq.mqserver.core.Message;
import com.pdq.mq.mqserver.core.Router;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @Description:
 * @Auther: pdq
 */
@SpringBootTest
public class RouterTest {
    private Binding binding = null;
    private Message message = null;
    private Router router = null;

    @BeforeEach
    private void setUp() {
        binding = new Binding();
        message = new Message();
        router = new Router();
    }

    @AfterEach
    private void tearDown() {
        binding = null;
        message = null;
        router = null;
    }


    // [测试用例]
    // binding key          routing key         result
    // aaa                  aaa                 true
    // aaa.bbb              aaa.bbb             true
    // aaa.bbb              aaa.bbb.ccc         false
    // aaa.bbb              aaa.ccc             false
    // aaa.bbb.ccc          aaa.bbb.ccc         true
    // aaa.*                aaa.bbb             true
    // aaa.*.bbb            aaa.bbb.ccc         false
    // *.aaa.bbb            aaa.bbb             false
    // #                    aaa.bbb.ccc         true
    // aaa.#                aaa.bbb             true
    // aaa.#                aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.ccc             true
    // aaa.#.ccc            aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.aaa.bbb.ccc     true
    // #.ccc                ccc                 true
    // #.ccc                aaa.bbb.ccc         true
    @Test
    public void test1() throws MqException {
        binding.setBindingKey("aaa");
        message.setRoutingKey("aaa");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test2() throws MqException {
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test3() throws MqException {
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test4() throws MqException {
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.ccc");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test5() throws MqException {
        binding.setBindingKey("aaa.bbb.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test6() throws MqException {
        binding.setBindingKey("aaa.*");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test7() throws MqException {
        binding.setBindingKey("aaa.*");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test8() throws MqException {
        binding.setBindingKey("aaa.*.bbb");
        message.setRoutingKey("aaa.ccc.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test9() throws MqException {
        binding.setBindingKey("aaa.*.bbb");
        message.setRoutingKey("aaa.ccc.ddd.bbb");
        Assertions.assertFalse(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test10() throws MqException {
        binding.setBindingKey("*.bb");
        message.setRoutingKey("aa.bb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test11() throws MqException {
        binding.setBindingKey("aaa");
        message.setRoutingKey("aaa");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test12() throws MqException {
        binding.setBindingKey("#");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test13() throws MqException {
        binding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test14() throws MqException {
        binding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test15() throws MqException {
        binding.setBindingKey("aaa.#.bbb");
        message.setRoutingKey("aaa.ccc.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

    @Test
    public void test16() throws MqException {
        binding.setBindingKey("aaa.#.bbb");
        message.setRoutingKey("aaa.ccc.fff.bbb");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test17() throws MqException {
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("bbb.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }
    @Test
    public void test18() throws MqException {
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");
        Assertions.assertTrue(router.route(ExchangeType.TOPIC,binding,message));
    }

}
