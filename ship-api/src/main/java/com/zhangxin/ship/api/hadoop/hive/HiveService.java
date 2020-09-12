package com.zhangxin.ship.api.hadoop.hive;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;

@Slf4j
public class HiveService {
    private Connection connection;
    private Statement statement;

    public HiveService() {
        init();
    }

    private void init() {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            connection = DriverManager.getConnection("jdbc:hive2://192.168.107.143:10000/test_db", "root", "abc123");
            statement = connection.createStatement();
        } catch (SQLException | ClassNotFoundException e) {
            log.error("创建Hive连接失败", e);
        }
    }

    public void close() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.error("close error", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("close error", e);
            }
        }
    }

    public void createDataBase() throws SQLException {
        String sql = "create database test_db";
        log.info("创建数据库，脚本：{}", sql);
        statement.execute(sql);
        log.info("创建数据库成功");
    }

    public void showDataBase() throws SQLException {
        String sql = "show databases";
        log.info("查询数据库，脚本：{}", sql);
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            log.info("查询到数据库：{}", rs.getString(1));
        }
    }

    public void createTable() throws SQLException {
        String sql = "create table user_tb(id int, name string) row format delimited fields terminated by ','";
        log.info("创建表，脚本：{}", sql);
        statement.execute(sql);
        log.info("创建表成功");
    }

    public void showTables() throws SQLException {
        String sql = "show tables";
        log.info("查询所有表，脚本：{}", sql);
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            log.info("查询到表：{}", rs.getString(1));
        }
    }

    public void showTableDetail() throws SQLException {
        String sql = "desc user_tb";
        log.info("查看表结构，脚本：{}", sql);
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            log.info("字段名：{}，类型：{}", rs.getString(1), rs.getString(2));
        }
    }

    public void loadData() throws SQLException {
        String sql = "load data local inpath '/home/data.txt' overwrite into table user_tb";
        log.info("导入数据，脚本：{}", sql);
        statement.execute(sql);
        log.info("导入数据成功");
    }

    public void selectData() throws SQLException {
        String sql = "select * from user_tb";
        log.info("查询数据，脚本：{}", sql);
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            log.info("id={},name={}", rs.getInt("id"), rs.getString("name"));
        }
    }

    public void dropTable() throws SQLException {
        String sql = "drop table if exists user_tb";
        log.info("删除表，脚本：{}", sql);
        statement.execute(sql);
        log.info("删除表成功");
    }

    public void dropDataBase() throws SQLException {
        String sql = "drop database if exists test_db";
        log.info("删除数据库，脚本：{}", sql);
        statement.execute(sql);
        log.info("删除数据库成功");
    }
}