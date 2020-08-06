package com.msg.mq.rocketmq.producer;
import java.util.UUID;

import com.msg.mq.rocketmq.exception.RocketMqException;
import com.msg.mq.rocketmq.utils.RocketMqSystemEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.msg.mq.rocketmq.constants.RocketMqConnectConstant;
import com.msg.mq.rocketmq.constants.RocketMqErrorEnum;

/**
 * 生产者配置
 * 
 * @Author wanshuang
 * @Email wanshuang@scqcp.com
 * @Date 2019年03月05日
 * @Version V1.0
 */
@Configuration
public class RocketMqProducerConfig {
	
    public static final Logger logger = LoggerFactory.getLogger(RocketMqProducerConfig.class);
    
    @Bean
    public DefaultMQProducer getRocketMQProducer() throws RocketMqException {
    	
    	String producerIsOnOff = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.producer_isOnOff);
    	if(StringUtils.isEmpty(producerIsOnOff)
    			|| "off".equalsIgnoreCase(producerIsOnOff)) {
    		return null;
    	}
    	
    	String groupName = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.producer_groupName);
    	if(StringUtils.isEmpty(groupName)) {
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL,"producer groupName is blank",false);
        }
        
    	String namesrvAddr = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.producer_namesrvAddr);
        if(StringUtils.isEmpty(namesrvAddr)) {
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL,"producer nameServerAddr is blank",false);
        }
        
        DefaultMQProducer producer;
        producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setVipChannelEnabled(false); 
        
        //如果需要同一个jvm中不同的producer往不同的mq集群发送消息，需要设置不同的instanceName
        producer.setInstanceName(UUID.randomUUID().toString());
        
        String maxMessageSize = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.producer_maxMessageSize);
        if(StringUtils.isEmpty(namesrvAddr)){
        	producer.setMaxMessageSize(Integer.valueOf(maxMessageSize));
        }
        String sendMsgTimeout = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.producer_sendMsgTimeout);
        if(StringUtils.isEmpty(sendMsgTimeout)){
        	producer.setSendMsgTimeout(Integer.valueOf(sendMsgTimeout));
        }
        String retryTimesWhenSendFailed = RocketMqSystemEnv.getPropertie(RocketMqConnectConstant.producer_retryTimesWhenSendFailed);
        if(StringUtils.isEmpty(retryTimesWhenSendFailed)){
        	producer.setRetryTimesWhenSendFailed(Integer.valueOf(retryTimesWhenSendFailed));
        }
        
        try {
            producer.start();
            logger.info(String.format("producer is start ! groupName:[%s],namesrvAddr:[%s]" , groupName, namesrvAddr));
        } catch (MQClientException e) {
        	logger.error(String.format("producer is error {}", e.getMessage(),e));
            throw new RocketMqException(e);
        }
        
        return producer;
    }
}