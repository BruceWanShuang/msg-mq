package com.msg.mq.rocketmq.consumer.client;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.msg.mq.rocketmq.bean.RocketMqConsumeResult;

public interface IRocketMqMessageProcessor {
    
	/**
     * 消费消息
     * 
     * 如果没有return true, consumer会重新消费该消息，直到return true
     * consumer可能重复消费该消息,请在业务端自己做是否重复调用处理,方法需要幂等处理
     * @param topic 消息主题
     * @param tag 消息标签
     * @param msgs 消息
     * @return
     */
	RocketMqConsumeResult handle(String topic, String tag, List<MessageExt> msgs) throws Exception;
}
