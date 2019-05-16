package com.sxt.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartition extends Partitioner<TQ,IntWritable>{

    @Override
    public int getPartition(TQ tq, IntWritable intWritable, int numPartitions) {
        return tq.getYear() % numPartitions;
    }
}
