package com.sxt.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TGroupComparator extends WritableComparator{

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ tqA = (TQ) a;
        TQ tqB = (TQ) b;

        //年月，从小到大排序
        int c = Integer.compare(tqA.getYear(),tqB.getYear());
        if (c ==0 ){
            c = Integer.compare(tqA.getMonth(),tqB.getMonth());
        }

        return c;
    }
}
