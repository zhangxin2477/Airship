package com.zhangxin.api;

import com.zhangxin.api.hadoop.HdfsService;
import com.zhangxin.api.kafka.KafkaService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

@SpringBootTest
class ApiApplicationTests {
    private static final Logger log = LoggerFactory.getLogger(ApiApplicationTests.class);
    @Resource
    private HdfsService hdfsService;

    @Test
    void contextLoads() {
        log.info("{}", hdfsService.mkdir("/zxnode_test"));
        hdfsService.upload(false, true, "/Users/zhangxin/nginx.conf", "/zxnode_test");
        log.info("{}", hdfsService.getFiles("/zxnode_test", null));
        log.info("{}", hdfsService.openWithString("/zxnode_test/nginx.conf"));
        hdfsService.download("/zxnode_test/nginx.conf", "/Users/zhangxin/test.txt");
        log.info("{}", (Object[]) hdfsService.getFileBlockLocations("/zxnode_test/nginx.conf"));

        kafkaService.createTopic("zxnode_test");
        log.info("{}", kafkaService.getTopics().names());
        kafkaService.deal("zxnode_test", "test");
        for (int i = 0; i < 4; i++) {
            kafkaService.send("zxnode_test", "test" + i);
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }
}