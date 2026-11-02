package com.pdq.mq.common;

import java.io.Serializable;

/**
 * @Description:
 * @Auther: pdq
 */
public class BasicReturns implements Serializable {
    //表示一次请求/响应 的身份标识,以便请求和响应一一匹配
    private String rid;
    //这次通信 channel 的身份标识
    private String channelId;
    //方法响应的返回值
    private boolean ok;

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }
}
