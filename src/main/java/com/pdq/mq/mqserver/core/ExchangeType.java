package com.pdq.mq.mqserver.core;

public enum ExchangeType {
    DIRECT(0),
    FANOUT(1),
    TOPIC(2);
    private final Integer id;
     ExchangeType(int id) {
        this.id = id;
    }

    public Integer getId() {
        return id;
    }
}
