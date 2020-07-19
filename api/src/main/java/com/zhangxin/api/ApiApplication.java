package com.zhangxin.api;

import com.zhangxin.api.hadoop.HdfsService;
import com.zhangxin.api.kafka.KafkaService;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.BasicConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.util.Properties;

@SpringBootApplication
public class ApiApplication {

    @Resource
    private RestTemplateBuilder restTemplateBuilder;

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }

    @Bean
    public RestTemplate restTemplate() {
        return restTemplateBuilder.build();
    }

    @Bean
    public HdfsService getHdfsService() {
        BasicConfigurator.configure();
        Configuration configuration = new Configuration();
        String hdfsUri = "hdfs://172.16.51.128:9000";
        configuration.set("fs.defaultFS", hdfsUri);
        return new HdfsService(configuration, hdfsUri);
    }

    @Bean
    public KafkaService getKafkaService() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.51.128:9092,172.16.51.129:9092,172.16.51.131:9092,172.16.51.132:9092");
        return new KafkaService(properties);
    }
}
