package com.zhangxin.ship.api.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long count = 0;
        //此时的输入是shuffle的输出
        //输入的key是字符串，输入的value是shuffle派发过来的是一个个list
        for (IntWritable v : values) {
            count += v.get();
        }
        context.write(key, new LongWritable(count));
    }
}