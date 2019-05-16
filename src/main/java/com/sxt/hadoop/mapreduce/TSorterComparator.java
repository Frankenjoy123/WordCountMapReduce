package com.sxt.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TSorterComparator extends WritableComparator{

    public TSorterComparator() {
        super(TQ.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TQ tqA = (TQ) a;
        TQ tqB = (TQ) b;

        //年月，从小到大排序
        int c = Integer.compare(tqA.getYear(),tqB.getYear());
        if (c ==0 ){
            c = Integer.compare(tqA.getMonth(),tqB.getMonth());
            if (c == 0){
                //温度从大到小排序
                c = Integer.compare(tqB.getWd() , tqA.getWd());
            }
        }

        return c;
    }
}
