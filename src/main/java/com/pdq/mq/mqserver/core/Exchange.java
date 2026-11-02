package com.pdq.mq.mqserver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description:这个类表示一个交换机
 * @Auther: pdq
 */
public class Exchange {
    //作为交换机的唯一身份标识
    private String name;
    //交换机类型，DIRECT，FANOUT，TOPIC
    //以枚举进行表示
    private ExchangeType exchangeType;
    //该交换机是否持久化
    private boolean durable = false;
    //如果该交换机没有人使用了，就会被删除,true表示为自动删除，false表示不自动删除
    //这个功能先写在这里，RabbitMQ中也有，但是目前只是列出来，并不实现
    private boolean autoDelete = false;

    //这个属性是创建交换机时传递的额外的参数，这个功能在后续代码中也不会实现,先列在这里（RabbitMQ中有）
    private Map<String,Object> arguments = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }

    public boolean getDurable() {
        return durable;
    }

    public void setDurable(Boolean durable) {
        this.durable = durable;
    }

    public boolean getAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(Boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    //以下两个get和set方法是针对数据库的存取
    //将arguments从HashMap类型转为String类型
    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this.arguments);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        //如果代码真的异常了，返回一个空的json字符串
        return "{}";
    }
    //将从数据库取出的arguments字符串转为map存储到exchange对象中
    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String,Object>>() {});
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

    //以下两个set和get方法是针对单元测试的,
    public void setArguments(String key,Object value) {
        arguments.put(key, value);
    }

    public Object getArguments(String key) {
        return arguments.get(key);
    }

    public void setArguments(Map<String,Object> arguments) {
        this.arguments = arguments;
    }
}
