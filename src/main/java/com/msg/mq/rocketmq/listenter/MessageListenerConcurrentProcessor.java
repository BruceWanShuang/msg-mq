package com.msg.mq.rocketmq.listenter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.msg.mq.common.annotation.SubscribeService;
import com.msg.mq.rocketmq.bean.RocketMqConsumeResult;
import com.msg.mq.rocketmq.exception.RocketMqException;
import org.apache.log4j.Logger;
import org.springframework.aop.framework.AdvisedSupport;
import org.springframework.aop.framework.AopProxy;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.msg.mq.rocketmq.constants.RocketMqErrorEnum;
import com.msg.mq.rocketmq.consumer.client.IRocketMqMessageProcessor;

/**
 * 并发消费路由
 * @Author wanshuang
 * @Email wanshuang@scqcp.com
 * @Date 2019年03月05日
 * @Version V1.0
 */
@Component
public class MessageListenerConcurrentProcessor implements MessageListenerConcurrently {

	private static final Logger logger = Logger
			.getLogger(MessageListenerConcurrentProcessor.class);

	@Autowired
	CousumerServiceListener cousumerServiceListener;

	/**
	 * 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息 不要抛异常,如果没有return
	 * CONSUME_SUCCESS,consumer会重新消费该消息,直到return CONSUME_SUCCESS
	 */
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {

		if (CollectionUtils.isEmpty(msgs)) {
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}

		ConsumeConcurrentlyStatus concurrentlyStatus = ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		try {

			// ------- 根据Topic分组 start -------
			Map<String, List<MessageExt>> topicGroups = new HashMap<>();
			for (MessageExt vo : msgs) {
				List<MessageExt> exts = topicGroups.get(vo.getTopic());
				if (exts == null) {
					exts = new ArrayList<>();
				}
				exts.add(vo);
				topicGroups.put(vo.getTopic(), exts);
			}
			// ------- 根据Topic分组 end -------

			for (Entry<String, List<MessageExt>> topicEntry : topicGroups
					.entrySet()) {
				String topic = topicEntry.getKey();
				// ------- 根据tags分组 start -------
				Map<String, List<MessageExt>> tagGroups = new HashMap<>();
				for (MessageExt vo : topicEntry.getValue()) {
					List<MessageExt> exts = topicGroups.get(vo.getTags());
					if (exts == null) {
						exts = new ArrayList<>();
					}
					exts.add(vo);
					tagGroups.put(vo.getTags(), exts);
				}
				// ------- 根据tags分组 end -------

				for (Entry<String, List<MessageExt>> tagEntry : tagGroups
						.entrySet()) {
					String tag = tagEntry.getKey();
					// 消费Topic下的tag的消息
					this.consumeMsgForTag(topic, tag, tagEntry.getValue());
				}
			}
		} catch (Exception e) {
			logger.error("消费处理异常，原因", e);
			concurrentlyStatus = ConsumeConcurrentlyStatus.RECONSUME_LATER;
		}

		// 如果没有return success ，consumer会重新消费该消息，直到return success
		return concurrentlyStatus;
	}

	/**
	 * 根据topic和 tags路由，查找消费消息服务
	 * 
	 * @param topic
	 * @param tag
	 * @param value
	 * @throws Exception
	 */
	private void consumeMsgForTag(String topic, String tag,
			List<MessageExt> value) throws Exception {

		// 根据topic和tag查询具体的消费服务集合
		List<IRocketMqMessageProcessor> rocketMqMessageProcessors = selectConsumeService(
				topic, tag);

		for (IRocketMqMessageProcessor iRocketMqMessageProcessor : rocketMqMessageProcessors) {
			try {
				if (iRocketMqMessageProcessor == null) {
					logger.error(String.format(
							"根据Topic:%s和Tag:%s 没有找到对应的处理消息的服务", topic, tag));
					throw new RocketMqException(
							RocketMqErrorEnum.PRODUCER_NOTFOUND_CONSUMESERVICE);
				}

				logger.info(String.format(
						"根据Topic:%s和Tag:%s路由到的服务为:%s,开始调用处理消息", topic, tag,
						iRocketMqMessageProcessor.getClass().getName()));
				long t1 = System.currentTimeMillis();

				// 调用该类的方法,处理消息
				RocketMqConsumeResult result = iRocketMqMessageProcessor
						.handle(topic, tag, value);
				long t2 = System.currentTimeMillis();

				if (result == null) {
					logger.error("消息处理失败：返回值为null" + ",MsgId="
							+ value.get(0).getMsgId() + ",耗时:" + (t2 - t1)
							+ "ms");
					throw new RocketMqException(
							RocketMqErrorEnum.PRODUCER_HANDLE_RESULT_NULL);
				}

				if (result.isSuccess()) {
					if (result.isSaveConsumeLog()) {
						logger.info("消息处理成功：" + JSON.toJSONString(result)
								+ ",MsgId=" + value.get(0).getMsgId() + ",耗时:"
								+ (t2 - t1) + "ms");
					}
				} else {
					if (result.isSaveConsumeLog()) {
						logger.info("消息处理失败：" + JSON.toJSONString(result)
								+ ",MsgId=" + value.get(0).getMsgId() + ",耗时:"
								+ (t2 - t1) + "ms");
					}
					throw new RocketMqException(
							RocketMqErrorEnum.PRODUCER_CONSUME_FAIL,
							JSON.toJSONString(result), false);
				}

			} catch (Exception e) {
				throw e;
			}
		}

	}

	/**
	 * 根据topic和tag查询对应的具体的消费服务
	 * 
	 * @param topic
	 * @param tag
	 * @return
	 */
	private List<IRocketMqMessageProcessor> selectConsumeService(String topic, String tag) {
		
		List<IRocketMqMessageProcessor> rocketMqMessageProcessors = new ArrayList<IRocketMqMessageProcessor>();
		
		//IRocketMqMessageProcessor rocketMqMessageProcessor = null;
		
		if(CousumerServiceListener.rocketMqMessageProcessors == null
				|| CousumerServiceListener.rocketMqMessageProcessors.size() == 0){
			throw new RocketMqException(RocketMqErrorEnum.PRODUCER_CONSUME_INIT_NOT_FINISH);
		}
		
		for (Map.Entry<String, IRocketMqMessageProcessor> entry : CousumerServiceListener.rocketMqMessageProcessors
				.entrySet()) {
			
			// 获取service实现类上注解的topic和tags
			SubscribeService consumeService = null;
			try {
				consumeService = getTarget(entry.getValue()).getClass()
						.getAnnotation(SubscribeService.class);
			} catch (Exception e) {
				logger.error("消费者服务获取目标对象异常", e);
			}
			
			if (consumeService == null) {
				logger.error("消费者服务：" + entry.getValue().getClass().getAnnotations().toString()
						+ "上没有添加MQConsumeService注解");
				continue;
			}
			String annotationTopic = consumeService.topic();
			if (!annotationTopic.equals(topic)) {
				continue;
			}
			
			String tagsArr[] = consumeService.tags();
			String tagsArrStr[] = new String[tagsArr.length];
			int i = 0;
			for (String taga : tagsArr) {
				tagsArrStr[i] = taga;
				i++;
			}
			
			// "*"号表示订阅该主题下所有的tag
			if (Arrays.asList(tagsArrStr).contains("*")) {
				// 获取该实例
				rocketMqMessageProcessors.add(entry.getValue());
			}
			
			boolean isContains = Arrays.asList(tagsArrStr).contains(tag);
			if (isContains) {
				// 获取该实例
				rocketMqMessageProcessors.add(entry.getValue());
			}
		}

		return rocketMqMessageProcessors;
	}

	/** 
	* 获取 目标对象 
	* 
	* @param proxy 代理对象 
	* @return 
	* @throws Exception 
	* @author wanshuang
	*/ 
	public static Object getTarget(Object proxy) throws Exception {
		
		if (!AopUtils.isAopProxy(proxy)) {
			return proxy;// 不是代理对象
		}
		
		if (AopUtils.isJdkDynamicProxy(proxy)) {
			return getJdkDynamicProxyTargetObject(proxy);
		} else { // cglib
			return getCglibProxyTargetObject(proxy);
		}
	}

	private static Object getCglibProxyTargetObject(Object proxy)
			throws Exception {
		
		Field h = proxy.getClass().getDeclaredField("CGLIB$CALLBACK_0");
		h.setAccessible(true);
		Object dynamicAdvisedInterceptor = h.get(proxy);
		Field advised = dynamicAdvisedInterceptor.getClass().getDeclaredField(
				"advised");
		advised.setAccessible(true);
		Object target = ((AdvisedSupport) advised
				.get(dynamicAdvisedInterceptor)).getTargetSource().getTarget();
		
		return target;
	}

	private static Object getJdkDynamicProxyTargetObject(Object proxy)
			throws Exception {
		
		Field h = proxy.getClass().getSuperclass().getDeclaredField("h");
		h.setAccessible(true);
		AopProxy aopProxy = (AopProxy) h.get(proxy);
		Field advised = aopProxy.getClass().getDeclaredField("advised");
		advised.setAccessible(true);
		Object target = ((AdvisedSupport) advised.get(aopProxy))
				.getTargetSource().getTarget();
		
		return target;
	}

}