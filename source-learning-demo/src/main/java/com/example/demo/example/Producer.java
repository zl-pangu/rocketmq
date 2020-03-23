package com.example.demo.example;

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
 * @date 2020/03/23
 **/
public class Producer {


    public static void main(String[] args) throws UnsupportedEncodingException, MQClientException, RemotingException, InterruptedException, MQBrokerException {

        //实例化生产者组名称。
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        //指定nameServer地址。
        producer.setNamesrvAddr("106.15.248.161:9876");

        //启动实例。
        producer.start();

        for (int i = 0; i < 1; i++) {
            //创建一个消息实例，指定主题，标签和消息正文。
            Message msg = new Message("TopicTest",
                    "TagA",
                    ("Hello RocketMQ " +
                            i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }

}
