package org.apache.flink.alphafish;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.util.*;

import static java.lang.Double.MAX_VALUE;

/**
 * 服务器版本
 */
public class LocalTestComplete {
    private static double EARTH_RADIUS = 6378.137;
    /**
     * 返回一个list，这个list表示这次随机后发送的目标节点列表
     * @param n 矩阵行数
     * @param m 矩阵列数
     * @param g 新创建的坐标点对象
     * @param r 随机数r
     * @return
     */
    public static List<Integer> partition2Matrix(int n, int m, GPSTrack g, Random r) {
        List<Integer> list = new ArrayList<>();
        if (g.isRowOrColumn() == true) {
            int rowID = r.nextInt(n) + 1;
            for (int i = 0; i < m; i++) {
                list.add(rowID * (m - 1) + i);
            }
        } else {
            int colId = r.nextInt(m) + 1;
            for (int i = 0; i < n; i++) {
                list.add(colId + i * m);
            }
        }
        return list;
    }

    /**
     * 经纬度换算
     * @param d
     * @return 后续计算值
     */
    private static double rad(double d) {
        return d * Math.PI / 180.0;
    }


    /**
     * 参数1：时间窗口大小 参数2：矩阵尺寸大小例如n*n  参数3：写入文件名
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

//        rw5 t = new rw5();
//        System.out.println("开始：");
//        t.getTimeByDate();

//        String starttime;
//        String endtime;
        /**
         * 参数1：时间窗口大小
         * 参数2：矩阵尺寸大小
         */
        final int windowTime = Integer.parseInt(args[0]);
        final int ROW_NUM = Integer.parseInt(args[1]);
        final int COL_NUM = Integer.parseInt(args[1]);
        List<Long> time = new ArrayList<Long>();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ROW_NUM*COL_NUM);

        /**
         * Kafka参数设置
         */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.131.199:9093,192.168.131.199:9094,192.168.131.199:9095");


//        starttime = new TimeStamp(System.currentTimeMillis());


        /**
         * 具体代码逻辑
         * 两条流
         */
        SingleOutputStreamOperator
                <GPSTrack> streamRow = env
                .addSource(new FlinkKafkaConsumer<>("sort1",
                new SimpleStringSchema(),properties).setStartFromEarliest())
                .setParallelism(1)
                .map((MapFunction<String, GPSTrack>) s -> {
                    //对每个点打上标签
                    GPSTrack g = new GPSTrack(s);
                    g.setRowOrColumn(true);//true是横向
                    Random r = new Random();
                    //设置了按照矩阵大小来分发到矩阵的各个节点上并返回一个列表
                    g.setIntoSID(partition2Matrix(ROW_NUM, COL_NUM, g, r));
                    return g;
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new WaterMark1()).process(new ProcessFunction<GPSTrack, GPSTrack>() {
                    @Override
                    public void processElement(GPSTrack gpsTrack, Context context, Collector<GPSTrack> collector) throws Exception {
                        List<Integer> list = gpsTrack.getIntoSID();
                        list.forEach(id -> {
                            gpsTrack.setPid(id);//Pid就是目标节点id
                            gpsTrack.setIntoSID(null);
                            collector.collect(gpsTrack);
                        });
                    }
                })
                .setParallelism(1);

        SingleOutputStreamOperator<GPSTrack> streamColumn = env
                .addSource(new FlinkKafkaConsumer<>("sort2",
                        new SimpleStringSchema(),properties).setStartFromEarliest())
                .setParallelism(1)
                .map((MapFunction<String, GPSTrack>) s -> {
                    GPSTrack g = new GPSTrack(s);
                    g.setRowOrColumn(false);
                    Random r = new Random();
                    g.setIntoSID(partition2Matrix(ROW_NUM, COL_NUM, g, r));
                    return g;
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new WaterMark1()).process(new ProcessFunction<GPSTrack, GPSTrack>() {
                    @Override
                    public void processElement(GPSTrack gpsTrack, Context context, Collector<GPSTrack> collector) throws Exception {
                        List<Integer> list = gpsTrack.getIntoSID();
                        list.forEach(id -> {
                            gpsTrack.setPid(id);
                            gpsTrack.setIntoSID(null);
                            collector.collect(gpsTrack);
                        });
                    }
                })
                .setParallelism(1);

        //计算轨迹相似度
        streamRow.union(streamColumn)
                .keyBy((KeySelector<GPSTrack, Integer>) gpsTrack -> gpsTrack.getPid())
                .timeWindow(Time.seconds(windowTime))
                .apply(new WindowFunction<GPSTrack, Tuple5<Integer, Integer, Integer, Integer, Double>, Integer, TimeWindow>() {
            /**
             * 根据 0 -1 计算两个list 多对多的值，形成tuple流出
             * @param integer
             * @param timeWindow
             * @param iterable
             * @param collector
             * @throws Exception
             */
            @Override
            public void apply(Integer integer, TimeWindow timeWindow, Iterable<GPSTrack> iterable,
                              Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
                for (GPSTrack i : iterable) {
                    //double dist = MAX_VALUE;
                    for (GPSTrack j : iterable) {
                        if (i.isRowOrColumn() == true && j.isRowOrColumn() == false) {

                            double dist = getDistance(i.getLatitude(), i.getLongitude(), j.getLatitude(), j.getLongitude());
                            /**
                             * 朴素的降负载方法，就是在计算过程中舍弃较大距离
                             */
//                            if (temp < dist) {
                                //dist = temp;
                            Tuple5<Integer, Integer, Integer, Integer, Double> element =
                                        new Tuple5(i.getUid(), j.getUid(), i.getId(), j.getId(), dist);
                            collector.collect(element);
                            //}
                        }
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(windowTime))
                .apply(new WindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>,String, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> iterable,
                                      Collector<String> collector) throws Exception {
                        //System.out.println(tuple.getField(0).toString());
                        /**
                         * 备用
                         */
                        // Stream<Tuple5<Integer, Integer, Integer, Integer, Double>> tuplestream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable.iterator(),Spliterator.SORTED), false);
                        // tuplestream.distinct().collect(Collectors.groupingBy());
                        /**
                         * @key: T2ID
                         * @value: Map<Integer ,   List < Double>> i.e.,PoID, distList
                         */

                        Map<Integer, Map<Integer, List<Double>>> map = new HashMap<>();
                        iterable.forEach(t -> {
                                    if (map.containsKey(t.f1)) {
                                        Map<Integer, List<Double>> maptemp = map.get(t.f1);
                                        if (maptemp.containsKey(t.f2)) {
                                            List<Double> listtemp = maptemp.get(t.f2);
                                            listtemp.add(t.f4);
                                            maptemp.replace(t.f2, listtemp);

                                        } else {
                                            List<Double> list = new ArrayList<>();
                                            list.add(t.f4);
                                            maptemp.put(t.f2, list);
                                        }


                                        map.replace(t.f1, maptemp);
                                    } else {
                                        Map<Integer, List<Double>> maptemp = new HashMap<Integer, List<Double>>();
                                        List<Double> list = new ArrayList<>();
                                        list.add(t.f4);
                                        maptemp.put(t.f2, list);
                                        map.put(t.f1, maptemp);
                                    }
                                }
                        );
                        /**
                         *
                         */
                        map.forEach((id, mapp) -> {
                            /**
                             * maptemp存了最终结果的距离
                             */
                            Map<String, Double> maptemp = new HashMap<>();
                            maptemp.put("max", Double.MIN_VALUE);
                            mapp.forEach((pid, list) -> {
                                /**
                                 * min：得到该点到另一条轨迹的min
                                 */
                                Double min = list.stream().min((o1, o2) -> o2.compareTo(o1)).get();
                                if (maptemp.get("max") < min)
                                    maptemp.replace("max", min);
                            });
//                            collector.collect(new Tuple3<Integer, Integer, Double>(tuple.getField(0), id, maptemp.get("max")));
                            time.add(System.currentTimeMillis());
                            collector.collect(String.valueOf((time.get(time.size()-1)-time.get(0))));
                            if(time.size()>1)
                                collector.collect(String.valueOf((time.get(time.size()-1)-time.get(0))/time.size()));

                        });
                    }
                })
                .writeAsText("/home/chenliang/data/pzc/outByPZC"+args[0]+"_"+args[1]+"_"+".txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();
    }

//    static class rw5{
//        public void getTimeByDate(){
//            Date date = new Date();
//            //DateFormat df1 = DateFormat.getDateInstance();
////日期格式，精确到日 例:2018-8-27
//            //System.out.println(df1.format(date));
//            //DateFormat df2 = DateFormat.getDateTimeInstance();
////可以精确到时分秒 例:2018-8-27 17:08:53
//            //System.out.println(df2.format(date));
//            //DateFormat df3 = DateFormat.getTimeInstance();
////只显示出时分秒 例：17:08:53
//            //System.out.println(df3.format(date));
//            DateFormat df4 = DateFormat.getDateTimeInstance(DateFormat.FULL,DateFormat.FULL);
////显示日期，周，上下午，时间（精确到秒）例：2018年8月27日 星期一 下午05时08分53秒 CST(CST为标准时间)
//            System.out.println(df4.format(date));
//            //DateFormat df5 = DateFormat.getDateTimeInstance(DateFormat.LONG,DateFormat.LONG);
//            //显示日期,上下午，时间（精确到秒） 例：2018年8月27日 下午05时08分53秒
//            //System.out.println(df5.format(date));
//            // DateFormat df6 = DateFormat.getDateTimeInstance(DateFormat.SHORT,DateFormat.SHORT);
////显示日期，上下午,时间（精确到分） 例：18-8-27 下午5:08
//            //System.out.println(df6.format(date));
//            //DateFormat df7 = DateFormat.getDateTimeInstance(DateFormat.MEDIUM,DateFormat.MEDIUM);
//            //显示日期，时间（精确到分） 例：2018-8-27 17:08:53
//            //System.out.println(df7.format(date));
//        }
//    }
    /**
     * Calculate distance of two points represented by lat/lng
     * @param lat1
     * @param lng1
     * @param lat2
     * @param lng2
     * @return
     */
    public static double getDistance(double lat1, double lng1, double lat2,
                                     double lng2) {
        double radLat1 = rad(lat1);
        double radLat2 = rad(lat2);
        double a = radLat1 - radLat2;
        double b = rad(lng1) - rad(lng2);
        double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
                + Math.cos(radLat1) * Math.cos(radLat2)
                * Math.pow(Math.sin(b / 2), 2)));
        s = s * EARTH_RADIUS;
        s = Math.round(s * 10000d) / 10000d;
        s = s * 1000;
        s = (double) Math.round(s * 100) / 100;//四舍五入保留小数

        return s;
    }
}





