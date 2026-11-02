package com.pdq.mq.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pdq.mq.common.ConsumerEnv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description:这个类表示一个队列
 * @Auther: pdq
 */
public class MSGQueue {
    //作为队列的唯一身份标识
    private String name;
    //该队列是否持久化
    private Boolean durable = false;
    //该队列在没人使用是否自动删除
    private Boolean autoDelete = false;
    //这个属性为true表示这个队列只能被一个消费者使用，其他消费者不能使用
    //这个功能也是先列在这里，后续代码也不会实现（RabbitMQ中有）
    private Boolean exclusive = false;
    //这个属性表示创建队列时传递的额外参数
    //这个功能也是先列在这里，后续代码先不实现
    private Map<String, Object> arguments = new HashMap<>();

    //这个参数表示当前队列中都有哪些订阅者
    private List<ConsumerEnv> consumerEnvList = new ArrayList<>();

    //使用这个参数来记录当前选择的订阅者，方便实现轮询的策略
    //consumerIndex 涉及到自增操作，所以使用AtomicInteger 进行原子性操作保证线程安全
    private AtomicInteger consumerIndex = new AtomicInteger(0);

    //添加订阅者
    public void addConsumer(ConsumerEnv consumerEnv) {
        consumerEnvList.add(consumerEnv);
    }

    //删除订阅者暂时先不考虑
    //挑选一个订阅者去发送消息(按照轮询的方式）
    public ConsumerEnv chooseConsumer(MSGQueue queue) {
        if (consumerEnvList.size() == 0) {
            //此时该队列并没有订阅者
            return null;
        }
        int index = consumerIndex.get() % consumerEnvList.size();
        consumerIndex.getAndIncrement();
        return consumerEnvList.get(index);
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getDurable() {
        return durable;
    }

    public void setDurable(Boolean durable) {
        this.durable = durable;
    }

    public Boolean getAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(Boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public Boolean getExclusive() {
        return exclusive;
    }

    public void setExclusive(Boolean exclusive) {
        this.exclusive = exclusive;
    }

    //将MSGQueue对象中的arguments从HashMap转为String类型
    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this.arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        //如果代码真的发生异常，返回空的json字符串
        return "{}";
    }

    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<Map<String, Object>>() {
            });
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    //以下两个set和get方法是针对单元测试的,
    public void setArguments(String key, Object value) {
        arguments.put(key, value);
    }

    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }
}
