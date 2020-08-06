package com.msg.mq.rocketmq.producer;

import java.util.Map;

import com.msg.mq.common.bean.BaseMqMessageDto;
import com.msg.mq.rocketmq.bean.RocketMqProducerResult;
import com.msg.mq.rocketmq.exception.RocketMqException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.msg.mq.rocketmq.constants.RocketMqErrorEnum;

/**
 * 生产者发送消息
 * 
 * @Author wanshuang
 * @Email wanshuang@scqcp.com
 * @Date 2019年03月05日
 * @Version V1.0
 */
@Component
public class RocketMqSendMsgProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RocketMqSendMsgProcessor.class);

    @Autowired
    private DefaultMQProducer defaultMQProducer;

    /**
     * 发送消息,仅发送一次，不关心是否发送成功
     * 
     * @param topic 主题
     * @param tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
     * @param keys 消息关键词，多个Key用MessageConst.KEY_SEPARATOR隔开（查询消息使用）
     * @param msg 消息
     */
    public void sendOneway(String topic, String tag, String keys, BaseMqMessageDto msg) {
        this.sendOneway(topic, tag, keys, JSON.toJSONString(msg));
    }

    /**
     * 发送消息,仅发送一次，不关心是否发送成功
     * 
     * @param topic 主题
     * @param tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
     * @param keys 消息关键词，多个Key用MessageConst.KEY_SEPARATOR隔开（查询消息使用）
     * @param msg 消息
     */
    public void sendOneway(String topic, String tag, String keys, String msg) {

        Message sendMsg = null;

        try {
            validateSendMsg(topic, tag, msg);
            sendMsg = new Message(topic, tag == null ? null : tag,
                    StringUtils.isEmpty(keys) ? null : keys, msg.getBytes());
            // 默认3秒超时
            defaultMQProducer.sendOneway(sendMsg);
        } catch (Exception e) {
            logger.error(String.format("sendOneway消息发送失败,Message:%s", sendMsg.toString()), e);
        }
    }

    /**
     * 同步发送消息(不带关键词，即时投递)
     * 
     * @param topic 主题
     * @param tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
     * @param msg 消息
     * @return
     */
    public RocketMqProducerResult send(String topic, String tag, String msg) {
        return this.send(topic, tag, null, msg, 0);
    }

    /**
     * 同步发送消息
     * 
     * @param topic 主题
     * @param tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
     * @param keys 消息关键词，多个Key用MessageConst.KEY_SEPARATOR隔开（查询消息使用）
     * @param msg 消息
     * @param delayTimeLevel 延时级别 (0代表不延时投递) 默认延时参考 messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     *            eg:delayTimeLevel=5代表消息延时1分钟投递至消费端
     * @return
     */
    public RocketMqProducerResult send(String topic, String tag, String keys, String msg, int delayTimeLevel) {

        RocketMqProducerResult result = null;

        Message sendMsg = null;
        try {
            validateSendMsg(topic, tag);
            SendResult sendResult = null;
            sendMsg = new Message(topic, tag, StringUtils.isEmpty(keys) ? null : keys, msg.getBytes());
            sendMsg.setDelayTimeLevel(delayTimeLevel);
            // 默认3秒超时
            sendResult = defaultMQProducer.send(sendMsg);
            result = new RocketMqProducerResult(sendResult);
        } catch (Exception e) {
            logger.error(String.format("send消息发送失败,Message:%s", sendMsg.toString()), e);
            result = new RocketMqProducerResult(e.getMessage(), null);
        }

        return result;
    }

    /**
     * 校验参数
     * 
     * @param topic
     * @param tag
     * @param msg
     */
    private void validateSendMsg(String topic, String tag, String msg) {
        if (StringUtils.isBlank(topic)) {
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL, "topic为空", false);
        }
        if (StringUtils.isBlank(tag)) {
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL, "tag为空", false);
        }
    }

    /**
     * 校验参数
     * 
     * @param topic
     * @param tag
     */
    private void validateSendMsg(String topic, String tag) {
        if (StringUtils.isBlank(topic)) {
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL, "topic为空", false);
        }
        if (StringUtils.isBlank(tag)) {
            throw new RocketMqException(RocketMqErrorEnum.PARAMM_NULL, "tag为空", false);
        }
    }

    /**
     * 同步发送消息
     * 
     * @param topic 主题
     * @param tag 消息标签，只支持设置一个Tag（服务端消息过滤使用）
     * @param keys 消息关键词，多个Key用MessageConst.KEY_SEPARATOR隔开（查询消息使用）
     * @param msg 消息
     * @param delayTimeLevel 延时级别 (0代表不延时投递) 默认延时参考 messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     *            eg:delayTimeLevel=5代表消息延时1分钟投递至消费端
     * @return
     */
    public RocketMqProducerResult send(String topic, String tag, String keys, Map<String, Object> msg, int delayTimeLevel) {

        RocketMqProducerResult result = null;

        Message sendMsg = null;
        try {
            String msgStr = JSONObject.toJSONString(msg);
            validateSendMsg(topic, tag, msgStr);
            SendResult sendResult = null;
            sendMsg = new Message(topic, tag, StringUtils.isEmpty(keys) ? null : keys,
                    msgStr.getBytes());
            sendMsg.setDelayTimeLevel(delayTimeLevel);

            // 默认3秒超时
            sendResult = defaultMQProducer.send(sendMsg);
            result = new RocketMqProducerResult(sendResult);
        } catch (Exception e) {
            logger.error(String.format("send消息发送失败,Message:%s", sendMsg.toString()), e);
            result = new RocketMqProducerResult(e.getMessage(), null);
        }

        return result;
    }

    // @BoundaryLogger
    // public RocketMqProducerResult sendCall(TopicEnum topic, TagEnum tag, String keys, String msg) throws Exception{
    //
    // Message sendMsg = new Message(topic.getCode(), tag.getCode(),
    // StringUtils.isEmpty(keys)?null:keys, msg.getBytes());
    //
    // defaultMQProducer.send(sendMsg, new SendCallback() {
    //
    // @Override
    // public void onSuccess(SendResult sendResult) {
    // // TODO Auto-generated method stub
    //
    // }
    //
    // @Override
    // public void onException(Throwable e) {
    // // TODO Auto-generated method stub
    //
    // }
    // });
    // }

}