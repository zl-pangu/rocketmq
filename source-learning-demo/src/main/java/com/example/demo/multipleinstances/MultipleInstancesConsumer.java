package com.example.demo.multipleinstances;

import com.example.demo.utils.RocketConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author zhouliang
 * @date 2020/6/1 16:20
 */
public class MultipleInstancesConsumer {

    public static void main(String[] args) throws MQClientException {
        for (int i = 0; i < 4; i++) {
            String instanceName = "multiple_instances_" + i;
            DefaultMQPushConsumer consumer = buildMultipleInstancesConsumer(instanceName);
            consumer.start();
        }
        System.out.printf("Consumer Started.%n");
    }

    private static DefaultMQPushConsumer buildMultipleInstancesConsumer(String instanceName) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("multiple_instances_consumer_group");
        consumer.setNamesrvAddr(RocketConstant.NAME_SERVER);
        consumer.subscribe("multiple_instances_topic", "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messageExts, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), messageExts);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.setInstanceName(instanceName);
        return consumer;
    }
}
