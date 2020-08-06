package com.msg.mq.rocketmq.constants;

/**
 * 连接常量配置
 * 
 * @Author wanshuang
 * @Email wanshuang@scqcp.com
 * @Date 2019年03月05日
 * @Version V1.0
 */
public class RocketMqConnectConstant {
	
	/** 生产者是否打开 */
	public static final String producer_isOnOff = "rocketmq.producer.isOnOff";
	
	/** 生产者组名 */
	public static final String producer_groupName = "rocketmq.producer.groupName";
	
	/** 生产者实例地址 */
	public static final String producer_namesrvAddr = "rocketmq.producer.namesrvAddr";
	
	/** 生产者消息大小 */
	public static final String producer_maxMessageSize = "rocketmq.producer.maxMessageSize";
	
	/** 生产者消息发送超时时间 */
	public static final String producer_sendMsgTimeout = "rocketmq.producer.sendMsgTimeout";
	
	/** 生产者消息发送失败重试次数*/
	public static final String producer_retryTimesWhenSendFailed = "rocketmq.producer.retryTimesWhenSendFailed";
	
	/** 消费者是否打开 */
	public static final String consumer_isOnOff = "rocketmq.consumer.isOnOff";
	
	/** 消费者实例扫码包 */
	public static final String CONSUMER_SCAN_PACKAGE = "rocketmq.consumer.scan.package";
	
	/** 消费者组名 */
	public static final String consumer_groupName = "rocketmq.consumer.groupName";
	
	/** 消费者实例地址 */
	public static final String consumer_namesrvAddr = "rocketmq.consumer.namesrvAddr";
	
	/** 消费者消费线程池数量(最小) */
	public static final String consumer_consumeThreadMin = "rocketmq.consumer.consumeThreadMin";
	
	/** 消费者消费线程池数量(最大)*/
	public static final String consumer_consumeThreadMax = "rocketmq.consumer.consumeThreadMax";
	
	/** 消费者消息大小 */
	public static final String consumer_consumeMessageBatchMaxSize = "rocketmq.consumer.consumeMessageBatchMaxSize";
}
