package com.wmc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: WangMC
 * @date: 2019/8/2 17:07
 * @description:
 */
public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行数据
        String line = value.toString();

        // 清洗数据
        String etlString = ETLUtils.etlString(line);

        if (etlString != null) {
            k.set(etlString);
        }
        // 写出数据
        context.write(k, NullWritable.get());
    }
}
