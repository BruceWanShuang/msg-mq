package com.msg.mq.rocketmq.consumer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import com.msg.mq.common.annotation.SubscribeService;
import com.msg.mq.rocketmq.constants.RocketMqConnectConstant;
import com.msg.mq.rocketmq.constants.RocketMqErrorEnum;
import com.msg.mq.rocketmq.exception.RocketMqException;
import com.msg.mq.rocketmq.listenter.MessageListenerConcurrentProcessor;
import com.msg.mq.rocketmq.utils.ClassHelper;
import com.msg.mq.rocketmq.utils.RocketMqSystemEnv;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * 生产者配置
 * 
 * @Author wanshuang
 * @Email wanshuang@scqcp.com
 * @Date 2019年03月05日
 * @Version V1.0
 */
@Configuration
public class RocketMqConsumerConfig {
	
	private static final Logger logger = Logger.getLogger(RocketMqConsumerConfig.class);
    
    @Autowired
    private MessageListenerConcurrentProcessor mqMessageListenerProcessor;
    
    //所有主题和标记
  	public Map<String, Set<String>> topicTagsMaps = new ConcurrentHashMap<String, Set<String>>();
    
	@PostConstruct
	private void init() {
		String scanPackage = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.CONSUMER_SCAN_PACKAGE);
		// 初始化加载所有的topics和tags
		if(StringUtils.isNotEmpty(scanPackage)) {
			this.initWithPackage(scanPackage);
		}
	}
	
	/**
	 * 扫描指定包的topics和tags
	 * 
	 * @param packageName
	 */
	private void initWithPackage(String packageName) {
		Set<Class<?>> classes = ClassHelper.getClasses(packageName);
		if (classes != null) {
			for (Class<?> clazz : classes) {
				SubscribeService consumeService = clazz.getAnnotation(SubscribeService.class);
				if (consumeService == null){continue;}
				String annotationTopic = consumeService.topic();
				String annotationTags[] = consumeService.tags();
			    if(StringUtils.isNotEmpty(annotationTopic)
			    		&& annotationTags != null
			    		&& annotationTags.length > 0){
			    	Set<String> values = topicTagsMaps.get(annotationTopic);
			    	if(values == null){
			    		values = new HashSet<String>();
			    	}
			    	for (String tag : annotationTags) {
			    		values.add(tag);
					}
			    	
			    	topicTagsMaps.put(annotationTopic, values);
			    }
			
			}
		}
		
		logger.info(String.format("初始化完所有topics和tags,%s", topicTagsMaps.toString()));
	}
	
    @Bean
    public DefaultMQPushConsumer getRocketMQConsumer() throws RocketMqException {
        
    	String consumerIsOnOff = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.consumer_isOnOff);
    	if(StringUtils.isEmpty(consumerIsOnOff)
    			|| "off".equalsIgnoreCase(consumerIsOnOff)) {
    		return null;
    	}
    	
    	Map<String, Set<String>> topics = this.topicTagsMaps;
    	if(topics == null || topics.size() == 0){
    		return null;
    	}
    	
    	String groupName = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.consumer_groupName);
    	if(StringUtils.isEmpty(groupName)){
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL,"consumer groupName is null !!!",false);
        }
    	
    	String namesrvAddr = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.consumer_namesrvAddr);
        if(StringUtils.isEmpty(namesrvAddr)){
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL,"consumer namesrvAddr is null !!!",false);
        }
        
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(namesrvAddr);
        
        String consumeThreadMin = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.consumer_consumeThreadMin);
        if(StringUtils.isEmpty(consumeThreadMin)){
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL,"consumer consumeThreadMin is null !!!",false);
        }
        consumer.setConsumeThreadMin(Integer.valueOf(consumeThreadMin));
        String consumeThreadMax = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.consumer_consumeThreadMax);
        if(StringUtils.isEmpty(consumeThreadMax)){
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL,"consumer consumeThreadMax is null !!!",false);
        }
        consumer.setConsumeThreadMax(Integer.valueOf(consumeThreadMax));
		consumer.registerMessageListener(mqMessageListenerProcessor);
		
		/**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        /**
         * 设置消费模型，集群还是广播，默认为集群
         */
        //consumer.setMessageModel(MessageModel.CLUSTERING);
        
//        String consumeMessageBatchMaxSize = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.consumer_consumeMessageBatchMaxSize);
//    	if(StringUtils.isEmpty(consumeMessageBatchMaxSize)){
//            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL,"consumer consumeMessageBatchMaxSize is null !!!",false);
//        }
        consumer.setConsumeMessageBatchMaxSize(1);
        
        try {
        	/**
        	 * 设置该消费者订阅的主题和tag，如果是订阅该主题下的所有tag，则tag使用*；如果需要指定订阅该主题下的某些tag，则使用||分割，例如tag1||tag2||tag3
        	 */
        	for (Map.Entry<String, Set<String>> entry : topics.entrySet()) { 
        		 Set<String> tags = entry.getValue();
        		 if(tags.contains("*") && tags.size() > 1){
        			 logger.error(String.format("订阅消息topics:%s和tags:%s,"
         			 		+ "不能既订阅所有又单独订单订阅其中某一个,请检查消费实例的SubscribeService注解配置", entry.getKey(), StringUtils.join(tags.toArray(), "||")));
        			 throw new RocketMqException(RocketMqErrorEnum.CONSUMER_TAG_ERROR);
        		 }else{
        			 consumer.subscribe(entry.getKey(), StringUtils.join(entry.getValue().toArray(), "||"));
        			 logger.info(String.format("订阅消息topics:%s和tags:%s", entry.getKey(), StringUtils.join(tags.toArray(), "||")));
        		 }
        		
        	}
        	
            consumer.start();
        }catch (MQClientException e){
        	logger.error(String.format("consumer is error {}", e.getMessage(),e));
            throw new RocketMqException(e);
        }
        
        return consumer;
    }
}