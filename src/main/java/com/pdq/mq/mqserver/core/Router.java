package com.pdq.mq.mqserver.core;

import com.pdq.mq.common.MqException;

/**
 * @Description:
 * @Auther: pdq
 */
public class Router {

    public boolean checkRoutingKey(String routingKey) {
        //约定routingKey的组成
        //由字母、数字、下划线组成
        //使用 . 进行分割

        //routingKey为空是合法情况,比如使用扇出交换机
        if(routingKey == null) {
            return true;
        }
        for (int i = 0; i < routingKey.length(); i++) {
            char c = routingKey.charAt(i);
            if(c >= 'A' && c <= 'Z') {
                continue;
            }
            if(c >= 'a' && c <= 'z') {
                continue;
            }
            if(c >= '0' && c <= '9') {
                continue;
            }
            if(c == '.' || c == '_') {
                continue;
            }
            return false;
        }
        return true;
    }
    public boolean checkBindingKey(String bindingKey) {
        //约定bindingKey的组成
        //字母、数字、下划线
        //由 . 分割部分
        //允许存在 * 和 # 作为通配符. 但是通配符只能作为独立的分段

        //允许bindingKey为空字符串,比如使用扇出交换机或者直接交换机
        if(bindingKey == null) {
            return true;
        }
        for (int i = 0; i < bindingKey.length(); i++) {
            char c = bindingKey.charAt(i);
            if(c >= 'A' && c <= 'Z') {
                continue;
            }
            if (c >= 'a' && c <= 'z') {
                continue;
            }
            if(c >= '0' && c <= '9') {
                continue;
            }
            if(c == '.' || c == '#' || c == '_') {
                continue;
            }
            return false;
        }
        //约定以 . 分割的各部分相邻的两个字符不能存在以下情况
        // ##    #*    *#

        //检查以 . 分割的各个独立部分是否合法
        String[] words = bindingKey.split("\\.");
        for (String word : words) {
            if(word.length() > 1 && (word.contains("#") || word.contains("*"))) {
                return false;
            }
        }
        //约定以 . 分割的各部分相邻的两个字符不能存在以下情况
        // ##    #*    *#
        for (int i = 0; i < words.length-1; i++) {
            if(words[i].equals('#') && words[i+1].equals('#')) {
                return false;
            }
            if(words[i].equals('#') && words[i+1].equals('*')) {
                return false;
            }
            if(words[i].equals('*') && words[i+1].equals('#')) {
                return false;
            }
        }
        return true;
    }
    //判断是否能够转发
    public boolean route(ExchangeType exchangeType,Binding binding,Message message) throws MqException {
        if (exchangeType == ExchangeType.FANOUT) {
            return true;
        } else if (exchangeType == ExchangeType.TOPIC) {
           return routTopic(binding.getBindingKey(),message.getRoutingKey());
        }else {
throw new MqException("[Router] 交换机类型非法! exchangeType="+exchangeType);
        }

    }

    //routingKey参照bindingKey进行匹配
    public boolean routTopic(String bindingKey, String routingKey) {
        String[] bindWords = bindingKey.split("\\.");
        String[] routWords = routingKey.split("\\.");
        //使用双指针遍历两个数组
        int bindIndex = 0;
        int routIndex = 0;
        while(bindIndex < bindWords.length && routIndex < routWords.length) {
            if(bindWords[bindIndex].equals("*")) {
                //routingKey和bindingKey同时向前走一个单位
                routIndex++;
                bindIndex++;
                continue;
            }else if(bindWords[bindIndex].equals("#")) {
                //先判断bindWords有没有下一个位置,如果没有，代表routingKey可以匹配任意的内容，直接返回true
                if(bindIndex == bindWords.length-1) {
                    return true;
                }
                //拿到bindingKey#通配符的下一个字符串nextBindWord
                bindIndex ++;
                String nextBindWord = bindWords[bindIndex];
                //匹配成功标志，为1匹配成功，为0 匹配失败
                int flag = 0;
                //在routingKey中匹配字符串nextBindWord,并返回此时的下标
                routIndex = findNextMatch(routWords, nextBindWord, routIndex);

                if(routIndex == -1) {
                    //routingKey中没有匹配到nextBindWord,匹配失败
                    return false;
                }else {
                    //routKey在整个长度范围内匹配到了nextBindWord,继续下一个
                    //此时routDest已经自增,这里只需把bindDest自增
                    bindIndex++;
                    routIndex++;
                    continue;
                }

            }else {
                //没有* 或 # 通配符，直接比较字符串是否相同
                if(bindWords[bindIndex].equals(routWords[routIndex])) {
                    bindIndex++;
                    routIndex++;
                }else{
                    return false;
                }
            }
        }
        //双方都到达末尾，匹配成功
        if(bindIndex == bindWords.length && routIndex == routWords.length) {
            return true;
        }
        return false;
    }

    //在routingKey中找到目标字符串,找到返回该字符串位置的下标.找不到返回-1
    private int findNextMatch(String[] routWords, String nextBindWord, int routIndex) {
        while(routIndex < routWords.length) {
            if (routWords[routIndex].equals(nextBindWord)) {
                //匹配到了，返回该位置的下标
                return routIndex;
            }
            routIndex++;
        }
        //走到末尾还没有找到，返回-1
        return -1;
    }

}
