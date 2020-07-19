package com.zhangxin.api;

import com.zhangxin.api.hadoop.HdfsService;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;

@SpringBootApplication
public class ApiApplication {
    private final String hdfsUri = "hdfs://172.16.51.128:9000";

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
        configuration.set("fs.defaultFS", hdfsUri);
        return new HdfsService(configuration, hdfsUri);
    }

}
