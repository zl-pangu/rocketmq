package com.example.demo.broadcast;

import com.example.demo.utils.RocketConstant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @author zhouliang
 * @date 2020/03/30
 **/
public class BroadcastProducer {

    /**
     * 什么是广播模式?
     * 广播模式：正在向主题的所有订户发送一条消息。如果希望所有订阅者都收到有关主题的消息，那么广播是一个不错的选择。
     */

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr(RocketConstant.NAME_SERVER);
        producer.start();

        for (int i = 0; i < 100; i++) {
            Message msg = new Message("TopicTest",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();

    }
}
