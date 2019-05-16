package com.sxt.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TReducer extends Reducer<TQ,IntWritable, Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Text rKey = new Text();
    private IntWritable rValue = new IntWritable();

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
            StringBuilder builder = new StringBuilder(key.getYear()).append("-");
            builder.append(key.getMonth()).append("-");
            builder.append(key.getDay());

            if (flag ==0){
                //年月日
                rKey.set(builder.toString());
                //温度
                rValue.set(key.getWd());
                context.write(rKey,rValue);
                flag++;
                day = key.getDay();
            }

            //第二高，日变化
            if (flag !=0 && day != key.getDay()){
                //年月日
                rKey.set(builder.toString());
                //温度
                rValue.set(key.getWd());
                context.write(rKey,rValue);
                break;
            }


        }

        super.reduce(key, values, context);
    }

//    public void reduce(Text key, Iterable<IntWritable> values,
//                       Context context) throws IOException, InterruptedException {
//        int sum = 0;
//        for (IntWritable val : values) {
//            sum += val.get();
//        }
//        result.set(sum);
//        context.write(key, result);
//    }
}
