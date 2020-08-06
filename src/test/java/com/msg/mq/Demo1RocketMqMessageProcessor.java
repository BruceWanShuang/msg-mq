package com.msg.mq;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.msg.mq.rocketmq.bean.RocketMqConsumeResult;
import com.msg.mq.rocketmq.consumer.client.RocketMqMessageProcessor;

//@SubscribeService(topic=TopicEnum.Ticket, tags={TagEnum.Ticket_INS,TagEnum.Log}, sequence = 0)
public class Demo1RocketMqMessageProcessor extends RocketMqMessageProcessor{

	@Override
	protected RocketMqConsumeResult onMessage(String tag, String keys,
                                              MessageExt messageExt) {
		String msg = new String(messageExt.getBody());
		System.out.println("获取到的消息为----" + "1111----" + msg);
		// TODO 判断该消息是否重复消费（RocketMQ不保证消息不重复，如果你的业务需要保证严格的不重复消息，需要你自己在业务端去重）

		// 如果注解中tags数据中包含多个tag或者是全部的tag(*)，则需要根据tag判断是那个业务，
		// 如果注解中tags为具体的某个tag，则该服务就是单独针对tag处理的
		if (tag.equals("某个tag")) {
			// 做某个操作
		}
		// TODO 获取该消息重试次数
		int reconsume = messageExt.getReconsumeTimes();
		// 根据消息重试次数判断是否需要继续消费
		if (reconsume == 3) {// 消息已经重试了3次，如果不需要再次消费，则返回成功

		}
		RocketMqConsumeResult result = new RocketMqConsumeResult();
		result.setSuccess(true);
		return result;
	}

}