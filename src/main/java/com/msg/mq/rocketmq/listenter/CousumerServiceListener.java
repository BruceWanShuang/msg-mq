package com.msg.mq.rocketmq.listenter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.msg.mq.common.annotation.SubscribeService;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.msg.mq.rocketmq.consumer.client.IRocketMqMessageProcessor;

@Component
public class CousumerServiceListener implements
		ApplicationListener<ContextRefreshedEvent> {
	
	// 所有消费实例
	public static Map<String, IRocketMqMessageProcessor> rocketMqMessageProcessors = new ConcurrentHashMap<String, IRocketMqMessageProcessor>();

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		
		if (event.getApplicationContext().getParent() == null) {
			Map<String, Object> beans = event.getApplicationContext()
					.getBeansWithAnnotation(SubscribeService.class);
			for(Map.Entry<String, Object> entry: beans.entrySet()){
				rocketMqMessageProcessors.put(entry.getKey(),
						(IRocketMqMessageProcessor) entry.getValue());
			}
		}
		
	}
}
