package com.zhangxin.ship.api.redis;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

@Slf4j
public class RedisService {
    public RedisService() {
    }

    public void init() throws IOException {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // 最大连接数
        poolConfig.setMaxTotal(1);
        // 最大空闲数
        poolConfig.setMaxIdle(1);
        // 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
        // Could not get a resource from the pool
        poolConfig.setMaxWaitMillis(1000);
        Set<HostAndPort> nodes = new LinkedHashSet<>();
        nodes.add(new HostAndPort("192.168.83.128", 6379));
        nodes.add(new HostAndPort("192.168.83.128", 6380));
        nodes.add(new HostAndPort("192.168.83.128", 6381));
        nodes.add(new HostAndPort("192.168.83.128", 6382));
        nodes.add(new HostAndPort("192.168.83.128", 6383));
        nodes.add(new HostAndPort("192.168.83.128", 6384));
        JedisCluster cluster = new JedisCluster(nodes, poolConfig);
        String name = cluster.get("name");
        log.info(name);
        cluster.set("age", "18");
        log.info(cluster.get("age"));
        cluster.close();
    }
}
