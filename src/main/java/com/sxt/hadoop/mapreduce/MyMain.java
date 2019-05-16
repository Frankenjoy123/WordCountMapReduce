package com.sxt.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyMain {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);

        Job job =Job.getInstance(conf);
        job.setJarByClass(MyMain.class);

        job.setJobName("sxt-tianqi");
        Path path = new Path("/user/root/test.txt");
        FileInputFormat.addInputPath(job,path);

        Path output = new Path("/data/wc/output");
        //存在删除文件
        if (output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);

        //Map Task
        job.setMapperClass(TMapper.class);
        //map的kev vaule类型
        job.setMapOutputKeyClass(TQ.class);
        job.setMapOutputValueClass(IntWritable.class);
        //分区比较器
        job.setPartitionerClass(TPartition.class);
        //排序比较器，80%环形缓冲区溢写，快排
        job.setSortComparatorClass(TSorterComparator.class);

        //Map阶段的Combine，相当于reduce前置
        job.setCombinerClass(TReducer.class);
        job.setCombinerKeyGroupingComparatorClass(TGroupComparator.class);

        //Reduce Task
        job.setReducerClass(TReducer.class);
        job.setGroupingComparatorClass(TGroupComparator.class);

        //output的key value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }

}
