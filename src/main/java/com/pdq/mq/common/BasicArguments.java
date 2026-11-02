package com.pdq.mq.common;

import java.io.Serializable;

/**
 * @Description: 使用这个类表示方法的公共参数/辅助的字段.
 *              后续每个方法又会有一些不同的参数, 不同的参数再分别使用不同的子类来表示
 * @Auther: pdq
 */
public class BasicArguments implements Serializable {
    //表示一次请求/响应 的身份标识,以便请求和响应一一匹配
    protected String rid;
    //这次通信 channel 的身份标识
    protected String channelId;

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
}
