package util;

import java.io.Serializable;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;


public class Zifp_gen implements Serializable {
    private Random random = new Random(0);
    NavigableMap<Double, Integer> map;
    private static final double Constant = 1.0;

    public Zifp_gen(int nums, double skewness) {
        // create the TreeMap
        map = computeMap(nums, skewness);
    }

    //size为rank个数，skew为数据倾斜程度, 取值为0表示数据无倾斜，取值越大倾斜程度越高
    private static NavigableMap<Double, Integer> computeMap(
            int size, double skew) {
        NavigableMap<Double, Integer> map =
                new TreeMap<Double, Integer>();
        //总频率
        double div = 0;
        //对每个rank，计算对应的词频，计算总词频
        for (int i = 1; i <= size; i++) {
            //the frequency in position i
            div += (Constant / Math.pow(i, skew));
        }
        //计算每个rank对应的y值，所以靠前rank的y值区间远比后面rank的y值区间大
        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (Constant / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

//    public int next() {         // [1,n]
//        double value = random.nextDouble();
//        //找最近y值对应的rank
//        return map.ceilingEntry(value).getValue() + 1;
//    }

}

