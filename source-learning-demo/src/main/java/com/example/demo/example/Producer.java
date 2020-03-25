package com.example.demo.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @author zhouliang
 * @date 2020/03/23
 **/
@Slf4j
public class Producer {


    public static void main(String[] args) {
        //同步消息发送
        try {
            syncSendMsg();
        } catch (MQClientException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        }
        //异步消息发送
//            asyncSendMsg();
    }

    /**
     * 官网示例：同步消息发送
     * 说明：在重要的通知消息，SMS通知，SMS营销系统等广泛的场景中使用可靠的同步传输。
     *
     * @throws MQClientException
     * @throws UnsupportedEncodingException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    private static void syncSendMsg() throws MQClientException, UnsupportedEncodingException,
            RemotingException, InterruptedException, MQBrokerException {

        //实例化生产者组名称：每个实例只有一个唯一的生产组
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        //指定nameServer地址。
        producer.setNamesrvAddr("localhost:9876");

        //启动实例。
        producer.start();

        try {
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
        } finally {
            producer.shutdown();
        }
    }

    /**
     * 官网示例：异步消息传输
     * <p>
     * 说明：异步传输通常用于对时间敏感的业务场景中。
     *
     * @throws MQClientException
     * @throws UnsupportedEncodingException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    private static void asyncSendMsg() throws MQClientException, UnsupportedEncodingException,
            RemotingException, InterruptedException, MQBrokerException {

        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        producer.setNamesrvAddr("127.0.0.1:9876");

        producer.start();

        producer.setRetryTimesWhenSendFailed(0);

        for (int i = 0; i < 1; i++) {
            Message message = new Message("TopicTest",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            producer.send(message, new AsyncSendMsgCallback(producer, i));
        }

        //官方示例是错的：异步消息发送之后，立即关闭的话，会导致
        // org.apache.rocketmq.remoting.netty.NettyRemotingClient.getAndCreateChannel
        //Channel channel = this.getAndCreateChannel(addr);注册失败
      /*
        //返回false
        ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
        log.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
        cw = new ChannelWrapper(channelFuture);
        this.channelTables.put(addr, cw);*/
//      producer.shutdown();

    }

    static class AsyncSendMsgCallback implements SendCallback {

        private DefaultMQProducer producer;
        private int index;

        private AsyncSendMsgCallback(DefaultMQProducer producer, int index) {
            this.producer = producer;
            this.index = index;
        }

        @Override
        public void onSuccess(SendResult sendResult) {
            System.out.printf("%-10d OK %s %n", index,
                    sendResult.getMsgId());

            producer.shutdown();
        }

        @Override
        public void onException(Throwable e) {
            System.out.printf("%-10d Exception %s %n", index, e);
            e.printStackTrace();

            producer.shutdown();
        }
    }
}
