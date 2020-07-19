package com.zhangxin.api;

import com.zhangxin.api.hadoop.HdfsService;
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
    }

}
