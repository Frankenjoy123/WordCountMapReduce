package com.sxt.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by xiaowu.zhou@tongdun.cn on 2019/4/17.
 */
public class MyWC {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);

        Job job =Job.getInstance(conf);

        job.setJarByClass(MyWC.class);

        job.setJobName("sxt-wc");
        Path path = new Path("/user/root/test.txt");
        FileInputFormat.addInputPath(job,path);

        Path output = new Path("/data/wc/output");
        //存在删除文件
        if (output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        //map的kev vaule类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //output的key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);


    }

}
