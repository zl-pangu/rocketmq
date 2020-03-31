package com.example.demo.delay;

import com.example.demo.utils.RocketConstant;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author zhouliang
 * @date 2020/03/31
 **/
public class ScheduledMessageProducer {
    /**
     * 延迟消息：
     * rocketMQ的延迟消息发送其实是已发送就已经到broker端了，然后消费端会延迟收到消息。
     * <p>
     * RocketMQ 目前只支持固定精度的定时消息。
     * 延迟级别（18个等级）
     * 1到18分别对应1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     * <p>
     * 为什么不支持固定精度的定时消息？
     * 因为rocketMQ之所以性能好的原因是broker端做的事情很少，基本都交给业务端做，就是消费端。如果支持
     * 我们自定义的延迟时候，会很大程度消耗broker的性能。
     * <p>
     * 延迟的底层方法是用定时任务实现的。
     */
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");

        producer.setNamesrvAddr(RocketConstant.NAME_SERVER);

        producer.start();

        int totalMessagesToSend = 100;

        for (int i = 0; i < totalMessagesToSend; i++) {
            while (true){
                Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());
                //延迟等级
                message.setDelayTimeLevel(1);
                producer.send(message);
            }
        }

        producer.shutdown();
    }
}
