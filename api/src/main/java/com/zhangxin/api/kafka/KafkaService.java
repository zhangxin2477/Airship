package com.zhangxin.api.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaService {
    private final Properties properties;

    public KafkaService(Properties properties) {
        this.properties = properties;
    }

    public void createTopic(String topic) {
        AdminClient client = AdminClient.create(properties);
        //分区和副本数
        NewTopic newTopic = new NewTopic(topic, 2, (short) 2);
        client.createTopics(Collections.singletonList(newTopic));
        client.close();
    }

    public void deleteTopic(String topic) {
        AdminClient client = AdminClient.create(properties);
        client.deleteTopics(Collections.singletonList(topic));
        client.close();
    }

    public ListTopicsResult getTopics() {
        AdminClient client = AdminClient.create(properties);
        ListTopicsResult topics = client.listTopics();
        client.close();
        return topics;
    }

    public void send(String topic, String msg) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
        producer.send(record);
        producer.close();
    }

    public void deal(String topic, String groupId) {
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
        Thread thread = new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("{}", record.toString());
                    consumer.commitAsync();
                }
            }
        });
        thread.start();
    }
}
