package com.pdq.mq.common;

/**
 * @Description:
 * @Auther: pdq
 */
public class MqException extends Exception{
    public MqException(String reason) {
        super(reason);
    }
}
