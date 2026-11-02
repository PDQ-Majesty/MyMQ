package com.pdq.mq.mqserver.dao;

import com.pdq.mq.MqApplication;
import com.pdq.mq.mqserver.core.Binding;
import com.pdq.mq.mqserver.core.Exchange;
import com.pdq.mq.mqserver.core.ExchangeType;
import com.pdq.mq.mqserver.core.MSGQueue;
import com.pdq.mq.mqserver.mapper.MetaMapper;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.util.List;

/**
 * @Description: 通过这个类整合数据库操作
 * @Auther: pdq
 */
public class DataBaseManager {
    //要做的是从spring拿到现成的对象
    //这里不使用@Autowired 的原因????
    //为什么使用MqApplication启动类

    private MetaMapper metaMapper;

    //初始化数据库
    //这里没有通过构造方法进行初始化的原因是，构造方法是对类的属性进行初始化，一般不涉及太多的业务逻辑，
    // 此处的初始化带有业务逻辑，所以单独拎出来手动进行初始化
    public void init() {
        //手动的获取metaMapper对象
        metaMapper = MqApplication.context.getBean(MetaMapper.class);
        //判断数据库是否存在
        if(!ifDBExits()) {
            //数据库不存在，建库建表
            //先创建一个data目录
            File dataDir = new File("./data");
            dataDir.mkdir();
            createTable();
            //插入默认数据
            createDefaultData();
            System.out.println("[DataBaseManager] 数据库初始化完成");
        } else {
            //数据库存在，不做操作
            System.out.println("[DataBaseManager] 数据库已经存在");
        }
    }

    //判断数据库是否存在
    //判断依据：yml配置文件中指定了数据源，也就是url中的meta.db，只需要判断./data/meta.db文件是否存在
    public Boolean ifDBExits() {
        File file = new File("./data/meta.db");
        return file.exists();
    }

    public void deleteDB() {
        File db = new File("./data/meta.db");
        Boolean ret = db.delete();
        if(ret) {
            System.out.println("[DataBaseManager] 删除数据库文件成功.......");
        }else {
            System.out.println("[DataBaseManager] 删除数据库文件失败.......");
        }

        //删除创建数据库产生的目录,确保该目录下没有数据
        File file = new File("./data");
        ret = file.delete();
        if(ret) {
            System.out.println("[DataBaseManager] 删除数据库目录成功.......");
        }else {
            System.out.println("[DataBaseManager] 删除数据库目录失败.......");
        }
    }


    public void createDefaultData() {
        //  给数据库中添加默认的数据
        //主要是给数据库中添加一个匿名交换机(DIRECT)，即把exchangeName设为空字符串
        //RabbitMQ中就有这样的设定
        Exchange exchange = new Exchange();
        exchange.setName("");
        exchange.setExchangeType(ExchangeType.DIRECT);
        exchange.setDurable(true);
        exchange.setAutoDelete(false);
        metaMapper.insertExchange(exchange);
    }
    public void createTable() {
        metaMapper.createExchangeTable();
        metaMapper.createQueueTable();
        metaMapper.createBindingTable();
    }

    //封装增删查方法
    public void insertExchange(Exchange exchange) {
        metaMapper.insertExchange(exchange);
    }
    public void deleteExchange(String name) {
        metaMapper.deleteExchange(name);
    }
    public List<Exchange> selectAllExchange() {
        return metaMapper.selectAllExchange();
    }

    public void insertQueue(MSGQueue queue) {
        metaMapper.insertQueue(queue);
    }
    public void deleteQueue(String name) {
        metaMapper.deleteQueue(name);
    }
    public List<MSGQueue> selectAllQueue() {
        return metaMapper.selectAllQueue();
    }

    public void insertBinding(Binding binding) {
        metaMapper.insertBinding(binding);
    }
    public void deleteBinding(Binding binding) {
        metaMapper.deleteBinding(binding);
    }
    public List<Binding> selectAllBinding() {
        return metaMapper.selectAllBinding();
    }


}
