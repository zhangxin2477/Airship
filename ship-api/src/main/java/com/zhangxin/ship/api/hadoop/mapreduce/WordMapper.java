package com.zhangxin.ship.api.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        //接收到split中的输出结果作为map的输入
        String line = value.toString();
        String[] words = line.split(" ");
        //放到list中，每个text统计加1，作为map的输出
        //写到context上下文环境中，输出到下一个执行过程shuffle
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}