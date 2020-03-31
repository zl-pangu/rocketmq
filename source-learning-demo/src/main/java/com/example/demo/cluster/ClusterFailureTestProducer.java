package com.example.demo.cluster;

import com.example.demo.utils.RocketConstant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;

/**
 * @author zhouliang
 * @date 2020/03/31
 **/
public class ClusterFailureTestProducer {

    public static void main(String[] args) throws MQClientException, RemotingException,
            InterruptedException, MQBrokerException, UnsupportedEncodingException {

        DefaultMQProducer producer = new DefaultMQProducer("cluster_producer_group");

        producer.setNamesrvAddr(RocketConstant.NAME_SERVER);

        producer.start();

        int i = 0;
        while (true) {
            String massage = "hello cluster！";
            Message message = new Message("cluster_test_topic", massage.getBytes(RemotingHelper.DEFAULT_CHARSET));
            //每隔两秒发送一次消息
            TimeUnit.MILLISECONDS.sleep(2000);
            producer.send(message);
            i++;
            if (i == 200) {
                break;
            }
        }
        producer.shutdown();
    }
}
