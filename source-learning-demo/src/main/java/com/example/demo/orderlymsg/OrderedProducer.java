package com.example.demo.orderlymsg;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author zhouliang
 * @date 2020/03/27
 **/
@Slf4j
public class OrderedProducer {

    /**
     * 下面的示例演示发送/接收全局和部分排序的消息
     *
     * @param args
     * @throws MQClientException
     * @throws UnsupportedEncodingException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException,
            RemotingException, InterruptedException, MQBrokerException {
        //构建生产者组
        DefaultMQProducer producer = new DefaultMQProducer("example_group_name");

        //nameServer
        producer.setNamesrvAddr("localhost:9876");

        //启动生产者
        producer.start();

        String[] tags = {"TagA", "TagB", "TagC", "TagD", "TagE"};

        for (int i = 0; i < 100; i++) {
            //RocketMQ可以严格的保证消息有序。但这个顺序，不是全局顺序，只是分区（queue）顺序。要全局顺序只能一个分区。
            //所以orderId取模一样的i都会进入一个队列里面
            //这个队列里面的顺序是可以保证的。
            //但是不能保证全局的顺序
            //想要保证全局的顺序，那么就把队列数设置为1吧，但是这样就牺牲了性能！
            int orderId = i % 10;
            //tags length = 5，所以i % tags.length = ,====>>>
            Message message = new Message("TopicTestjjj", tags[i % tags.length], "KEY" + i,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    //取模的hash 值，就是传入的orderId
                    Integer id = (Integer) arg;
                    //orderId按照队列长度取模，queue默认为4，所以index[0,3]
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, orderId);

            log.info("orderId===>>{},message====>>{},send：{}", orderId, message, sendResult);
        }

        //server showdown
        producer.shutdown();
    }
}
