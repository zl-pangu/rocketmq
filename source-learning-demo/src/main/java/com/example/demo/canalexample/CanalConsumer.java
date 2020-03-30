package com.example.demo.canalexample;

import com.example.demo.canalexample.dto.Data;
import com.example.demo.canalexample.dto.TestCanalDTO;
import com.example.demo.utils.JsonUtil;
import com.example.demo.utils.RocketConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhouliang
 * @date 2020/3/27 15:24
 */
@Slf4j
public class CanalConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("example-group");
        consumer.setNamesrvAddr(RocketConstant.NAME_SERVER);
        consumer.subscribe("gantrydata_async_topic", "*");
        //最大可以拉取的数量
        consumer.setConsumeMessageBatchMaxSize(1000);
        //每次拉取多少
        consumer.setPullBatchSize(500);
        //最小活跃线程数
        consumer.setConsumeThreadMin(20);
        //允许的最大线程数
        consumer.setConsumeThreadMax(64);
        //这样设置假设100条数据1个线程处理一条数据的时间为1秒，那么[1-3]之间。
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            AtomicInteger atomicInteger = new AtomicInteger(0);

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                long start = System.currentTimeMillis();
                for (MessageExt messageExt : msgs) {
                    atomicInteger.incrementAndGet();
                    byte[] body = messageExt.getBody();
                    try {
                        int queueId = messageExt.getQueueId();
                        String json = new String(body, RemotingHelper.DEFAULT_CHARSET);
                        TestCanalDTO testCanalDTO = JsonUtil.jsonToObject(json, TestCanalDTO.class);
                        List<Data> data = testCanalDTO.getData();
                        Data gantryData = data.get(0);
                        String contnrCode = gantryData.getContnrCode();
                        //验证canal配置：canal.mq.partitionHash=nuts_gantry\\.gantry_scan_data_0:contnr_code,.\\..*
                        log.info("queueId:{},contnrCode:{}", queueId, contnrCode);
                    } catch (UnsupportedEncodingException e) {
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                long end = System.currentTimeMillis();
                System.err.println("msgs size:" + msgs.size() + "=============================>>>>>>" + atomicInteger.get() + "耗时：" + (end - start));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //jvm退出的时候回调钩子函数，关闭consumer
        CloseMyConsumer closeMyConsumer = new CloseMyConsumer(consumer);
        Runtime.getRuntime().addShutdownHook(closeMyConsumer);
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    /**
     * 关闭consumer
     */
    private static class CloseMyConsumer extends Thread {

        private DefaultMQPushConsumer consumer;

        private CloseMyConsumer(DefaultMQPushConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            if (null != consumer) {
                consumer.shutdown();
            }
        }
    }
}
