package com.msg.mq.rocketmq.consumer.client;

import java.util.List;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.msg.mq.rocketmq.bean.RocketMqConsumeResult;

public abstract class RocketMqMessageProcessor implements IRocketMqMessageProcessor {

	@Override
	public RocketMqConsumeResult handle(String topic, String tag,
                                        List<MessageExt> msgs)  throws Exception {
		RocketMqConsumeResult result = new RocketMqConsumeResult();
		for (MessageExt messageExt : msgs) {
			result = this.onMessage(
					tag, messageExt.getKeys() == null ? null : messageExt.getKeys(), messageExt);
		}

		return result;
	}

	//钩子函数
	protected abstract RocketMqConsumeResult onMessage(String tag, String keys,
			MessageExt messageExt) throws Exception;

}
