package com.sxt.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

//优化，map输出2个
public class TCombiner extends Reducer<TQ,IntWritable, TQ,IntWritable> {

    @Override
    protected void reduce(TQ key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //相同的key为一组，年月为一组，根据分组比较器
        //1940 1 6 22
        //1940 1 3 21
        //1940 1 11 20
        //调用真.迭代器 nextKeyValue

        int flag = 0;
        int day=0;
        for (IntWritable val : values){
            if (flag ==0){
                context.write(key,val);
                flag++;
                day = key.getDay();
            }

            //第二高，日变化
            if (flag !=0 && day != key.getDay()){
                context.write(key,val);
                break;
            }
        }

    }

}
