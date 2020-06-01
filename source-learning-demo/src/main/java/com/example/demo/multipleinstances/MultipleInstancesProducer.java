package com.example.demo.multipleinstances;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @author zhouliang
 * @date 2020/6/1 16:26
 */
public class MultipleInstancesProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("multiple_instances_consumer_group");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0; i < 20; i++) {
            Message message = new Message("multiple_instances_topic"
                    , "TagA"
                    , ("Hello RocketMQ ").getBytes(RemotingHelper.DEFAULT_CHARSET));
            producer.send(message);
        }

        producer.shutdown();
    }
}
