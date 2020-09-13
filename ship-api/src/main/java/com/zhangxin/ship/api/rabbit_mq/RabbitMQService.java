package com.zhangxin.ship.api.rabbit_mq;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RabbitMQService {
    // 是否自动应答
    private final boolean autoAck = true;
    /**
     * 是否启用RabbitMQ
     */
    @Value("${rabbitmq.setting.enable}")
    private boolean enable;
    /**
     * 交换器名
     */
    @Value("${rabbitmq.setting.exchange.name}")
    private String exchangeName;
    /**
     * 下行队列名
     */
    @Value("down.${push.setting.school}")
    private String downQueueName;
    @Value("down.${push.setting.school}")
    private String downRoutingKey;
    @Value("up.${push.setting.school}")
    private String upQueueName;
    @Value("up.${push.setting.school}")
    private String upRoutingKey;
    @Value("${rabbitmq.setting.username}")
    private String username;
    @Value("${rabbitmq.setting.password}")
    private String password;
    @Value("${rabbitmq.setting.host}")
    private String host;
    @Value("${rabbitmq.setting.port}")
    private Integer port;
    private Channel channelDown;
    private Channel channelUp;
    private Connection upConnection;
    private Connection downConnection;
    @Value("${rabbitmq.setting.message-life-time}")
    private Integer messageLifeTime;

    // 初始化
    private void init() {
        if (!enable) return;
        try {
            // 创建连接
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setHost(host);
            factory.setPort(port);
            // 队列参数
            Map<String, Object> args = new HashMap<>();
            args.put("x-message-ttl", messageLifeTime * 1000);// 消息过期时间
            // 创建上行连接
            upConnection = factory.newConnection();
            // 创建上行通道
            channelUp = upConnection.createChannel();
            // 声明创建配置上行队列
            channelUp.queueDeclare(upQueueName, true, false, false, args);
            // 将队列与交换器绑定，并设置路由码
            channelUp.queueBind(upQueueName, exchangeName, upRoutingKey);
            downConnection = factory.newConnection();
            channelDown = downConnection.createChannel();
            channelDown.queueDeclare(downQueueName, true, false, false, args);
            channelDown.queueBind(downQueueName, exchangeName, downRoutingKey);
            receiveMessage();
        } catch (Exception e) {
            log.error("启动MQ下行通道时出现异常！", e);
        }
    }

    /**
     * 持续监听队列以接收数据
     *
     * @throws IOException
     * @throws TimeoutException
     */
    private void receiveMessage() throws IOException, TimeoutException {
        // 每次缓存5个消息在本地
        channelDown.basicQos(5);
        channelDown.basicConsume(downQueueName, autoAck, "myConsumerTag", new DefaultConsumer(channelDown) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, StandardCharsets.UTF_8);
                log.debug(downQueueName + " Received '" + message + "'" + ", routingKey: " + envelope.getRoutingKey());
                // 处理接收到的消息
                // 持续监听
                channelDown.basicConsume(downQueueName, autoAck, "myConsumerTag", this);
                channelDown.basicAck(envelope.getDeliveryTag(), true);
            }
        });
    }

    /**
     * 向上行消息队列发送一条消息
     *
     * @param message
     * @throws IOException
     * @throws TimeoutException
     */
    public void sendMessage(String message) throws IOException, TimeoutException {
        channelUp.basicPublish(exchangeName, upRoutingKey, true, MessageProperties.TEXT_PLAIN, message.getBytes());
        log.debug("send message to " + upQueueName + ": " + message);
    }
}
