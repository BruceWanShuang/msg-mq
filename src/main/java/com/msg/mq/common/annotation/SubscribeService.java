package com.msg.mq.common.annotation;

import org.springframework.stereotype.Service;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
@Service
public @interface SubscribeService {

	// 消息主题
	String topic();

	// 消息标签,如果是该主题下所有的标签，使用“*”
	String[] tags();

	// 消费顺序(暂时没用)
	int sequence();

}
