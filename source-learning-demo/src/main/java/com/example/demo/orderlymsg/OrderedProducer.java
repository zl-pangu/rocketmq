package com.example.demo.orderlymsg;

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
public class OrderedProducer {

    /**
     * 下面的示例演示发送/接收全局和部分排序的消息
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

            System.out.printf("%s%n", sendResult);
        }

        //server showdown

        producer.shutdown();
    }
}
