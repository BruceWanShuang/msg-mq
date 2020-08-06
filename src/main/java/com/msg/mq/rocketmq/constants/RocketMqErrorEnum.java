package com.msg.mq.rocketmq.constants;

import com.msg.mq.common.constants.ErrorCode;

public enum RocketMqErrorEnum implements ErrorCode {
	
	/********公共********/
	PARAMM_NULL("MQ_001","参数为空"),
	
	/********生产者*******/
	CONSUMER_TAG_ERROR("MQ_99","消费实例tag配置不被允许"),
	
	/********消费者*******/
	PRODUCER_NOTFOUND_CONSUMESERVICE("MQ_100","根据topic和tag没有找到对应的消费服务"),
	PRODUCER_HANDLE_RESULT_NULL("MQ_101","消费方法返回值为空"),
	PRODUCER_CONSUME_FAIL("MQ_102","消费失败"),
	PRODUCER_CONSUME_INIT_NOT_FINISH("MQ_103","消费实例未初始化完成");

    private String code;
    
    private String msg;

    private RocketMqErrorEnum(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }
    
    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getMsg() {
        return this.msg;
    }

}