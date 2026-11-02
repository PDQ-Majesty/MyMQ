package com.pdq.mq.mqserver.mapper;

import com.pdq.mq.mqserver.core.Binding;
import com.pdq.mq.mqserver.core.Exchange;
import com.pdq.mq.mqserver.core.MSGQueue;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * @Description:
 * @Auther: pdq
 * @Date: 2026/9/16 20:22
 */
@Mapper
public interface MetaMapper {
    //创建三个核心的表
    void createExchangeTable();
    void createQueueTable();
    void createBindingTable();
    //对应表的增删改查操作
    void insertExchange(Exchange exchange);
    void deleteExchange(String name);
    List<Exchange> selectAllExchange();
    void insertQueue(MSGQueue queue);
    void deleteQueue(String name);
    List<MSGQueue> selectAllQueue();
    void insertBinding(Binding binding);
    void deleteBinding(Binding binding);
    List<Binding> selectAllBinding();
}
