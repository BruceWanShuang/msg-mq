package com.msg.mq.rocketmq.listenter;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 顺序消费路由
 * @Author wanshuang
 * @Email wanshuang@scqcp.com
 * @Date 2020年08月06日 16:42
 * @Vsersion 1.0
 **/
public class MessageListenerOrderProcessor implements MessageListenerOrderly {
    @Override
    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {

        //ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;//延迟消费
        return ConsumeOrderlyStatus.SUCCESS;
    }
}
