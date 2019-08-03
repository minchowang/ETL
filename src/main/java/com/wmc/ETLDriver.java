package com.wmc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: WangMC
 * @date: 2019/8/2 21:36
 * @description:
 */
public class ETLDriver implements Tool {
    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {
        // 1、获取job对象
        Job job = Job.getInstance(configuration);
        // 2、封装driver类
        job.setJarByClass(ETLDriver.class);
        // 3、关联Mapper类
        job.setMapperClass(ETLMapper.class);
        // 4、Mapper输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 5、最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 设置执行流程不走Reduce任务
        // job.setNumReduceTasks(0);

        // 6、输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 7、提交任务
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public static void main(String[] args) throws Exception {
        int i = ToolRunner.run(new ETLDriver(), args);
        System.out.println(i);
    }
}
