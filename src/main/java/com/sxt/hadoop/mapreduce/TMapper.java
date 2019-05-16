package com.sxt.hadoop.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.text.ParseException;
import java.util.StringTokenizer;


public class TMapper
        extends Mapper<LongWritable, Text, TQ, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private TQ myKey = new TQ();
    private IntWritable myValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        // 2019-01-01   30c

        try {
            String[]  arr =  StringUtils.split(value.toString(),'\t');
            String dayStr = arr[0];
            String wdStr = arr[1];
            FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd");
            Date date = DateUtils.parseDate(dayStr, new String[]{"yyyy-MM-dd"});
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int year = calendar.get(Calendar.YEAR);
            int month = calendar.get(Calendar.MONTH)+1;
            int day = calendar.get(Calendar.DAY_OF_MONTH);
            int wd = Integer.parseInt(wdStr.substring(0,wdStr.length()-1));
            myKey.setYear(year);
            myKey.setMonth(month);
            myKey.setDay(day);
            myKey.setWd(wd);

            myValue.set(wd);
            context.write(myKey,myValue);
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }

//hello sxt 1
//    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//        StringTokenizer itr = new StringTokenizer(value.toString());
//        while (itr.hasMoreTokens()) {
//            word.set(itr.nextToken());
//            context.write(word, one);
//        }
//    }
}
