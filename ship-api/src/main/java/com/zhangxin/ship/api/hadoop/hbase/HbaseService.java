package com.zhangxin.ship.api.hadoop.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

@Slf4j
public class HbaseService {
    public Configuration configuration; // 管理Hbase的配置信息
    public Connection connection; // 管理Hbase连接
    public Admin admin; // 管理Hbase数据库的信息

    public void init() throws IOException {
        //单机模式
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        //集群模式
        configuration.set("hbase.rootdir", "hdfs://master:9000/hbase");//主节点
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2"); // 设置zookeeper节点
        configuration.set("hbase.zookeeper.property.clientPort", "2181"); // 设置客户端节点
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    // 操作数据库之后，关闭连接
    public void close() {
        try {
            if (admin != null) {
                admin.close(); // 退出用户
            }
            if (null != connection) {
                connection.close(); // 关闭连接
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    public void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            log.error("Table exists");
        } else {
            //表描述器构造器
            TableDescriptorBuilder table = TableDescriptorBuilder.newBuilder(tableName);
            for (String str : colFamily) {
                //列族描述器构造器
                ColumnFamilyDescriptorBuilder column = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(str));
                //获得列描述器
                ColumnFamilyDescriptor familyDescriptor = column.build();
                //添加列族
                table.setColumnFamily(familyDescriptor);
            }
            //获得表描述器
            TableDescriptor tableDescriptor = table.build();
            admin.createTable(tableDescriptor);
        }
    }

    //删除表
    public void deleteTable(String tableName) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        admin = connection.getAdmin();
        admin.disableTable(tablename);
        admin.deleteTable(tablename);
    }

    /**
     * 添加单元格数据
     *
     * @param tableName 表名
     * @param rowKey    行键
     * @param colFamily 列族
     * @param col       列限定符
     * @param val       数据
     */
    public void insertData(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }

    /**
     * 浏览数据
     *
     * @param tableName 表名
     * @param rowKey    行
     * @param colFamily 列族
     * @param col       列限定符
     */
    public void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(), col.getBytes());
        Result result = table.get(get);
        log.info(new String(result.getValue(colFamily.getBytes(), col.getBytes())));
        table.close();
    }
}
