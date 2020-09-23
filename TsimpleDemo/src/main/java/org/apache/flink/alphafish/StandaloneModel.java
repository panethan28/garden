package org.apache.flink.alphafish;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;

import static java.lang.Double.MAX_VALUE;


public class StandaloneModel {


    /**
     * Preparation for distanceCalculation
     */
    private static double EARTH_RADIUS = 6378.137;

    private static double rad(double d) {
        return d * Math.PI / 180.0;
    }

    /**
     * Join-Matrix Algorithm for real-time Trajectories.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        final int windowTime = 1000000;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);//预设矩阵大小

//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "192.168.131.199:9093,192.168.131.199:9094,192.168.131.199:9095");
        
        /**
         * 横向发送
         */
        SingleOutputStreamOperator
                <GPSTrack> streamRow = env.readTextFile("C:\\Users\\Ethan\\Desktop\\sort1.txt").map(new MapFunction<String, GPSTrack>() {
            @Override
            public GPSTrack map(String s) throws Exception {
                /**
                 * 打上横向还是纵向的标签
                 * 随机的发第一行还是第二行
                 */
                GPSTrack g = new GPSTrack(s);
                g.setRowOrColumn(true);//true是横向
                List<Integer> list = new ArrayList<>();//定义了进入节点的list
                int rd = Math.random() > 0.5 ? 1 : 0;

                if (rd == 0) {
                    list.add(1);
                    list.add(2);
                } else {
                    list.add(3);
                    list.add(4);
                }
                g.setIntoSID(list);
                return g;
            }
        }).setParallelism(1).assignTimestampsAndWatermarks(new WaterMark1()).process(new ProcessFunction<GPSTrack, GPSTrack>() {
            @Override
            public void processElement(GPSTrack gpsTrack, Context context, Collector<GPSTrack> collector) throws Exception {
                List<Integer> list = gpsTrack.getIntoSID();
                list.forEach(id -> {
                    gpsTrack.setPid(id);
                    gpsTrack.setIntoSID(null);
                    collector.collect(gpsTrack);
                });
            }
        });
        /**
         * 列方向的流
         * OUT: 带有目标地址的点
         */
        SingleOutputStreamOperator<GPSTrack> streamColumn = env.readTextFile("C:\\Users\\Ethan\\Desktop\\sort2.txt").map(new MapFunction<String, GPSTrack>() {
            @Override
            public GPSTrack map(String s) throws Exception {
                /**
                 * 打上横向还是纵向的标签
                 * 随机的发第一行还是第二行
                 */
                GPSTrack g = new GPSTrack(s);
                g.setRowOrColumn(false);//true是横向
                List<Integer> list = new ArrayList<>();//定义了进入节点的list
                int rd = Math.random() > 0.5 ? 1 : 0;

                if (rd == 0) {
                    list.add(1);
                    list.add(3);
                } else {
                    list.add(2);
                    list.add(4);
                }
                g.setIntoSID(list);
                return g;
            }
        }).setParallelism(1).assignTimestampsAndWatermarks(new WaterMark1()).process(new ProcessFunction<GPSTrack, GPSTrack>() {
            @Override
            public void processElement(GPSTrack gpsTrack, Context context, Collector<GPSTrack> collector) throws Exception {
                List<Integer> list = gpsTrack.getIntoSID();
                list.forEach(id -> {
                    gpsTrack.setPid(id);//pid这里暂指slotID
                    gpsTrack.setIntoSID(null);
                    collector.collect(gpsTrack);
                });
            }
        });
        /**
         * 计算轨迹相似度
         */
        streamRow.union(streamColumn).keyBy(new KeySelector<GPSTrack, Integer>() {
            @Override
            public Integer getKey(GPSTrack gpsTrack) throws Exception {
                return gpsTrack.getPid();
            }
        }).timeWindow(Time.seconds(windowTime)).apply(new WindowFunction<GPSTrack, Tuple5<Integer, Integer, Integer, Integer, Double>, Integer, TimeWindow>() {
            /**
             *
             * @param integer
             * @param timeWindow
             * @param iterable
             * @param collector
             * @throws Exception
             */
            @Override
            public void apply(Integer integer, TimeWindow timeWindow, Iterable<GPSTrack> iterable, Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
                /**
                 * 根据 0 -1 计算两个list 多对多的值，形成tuple流出
                 * OUT: tuple3< T1ID, T2ID, point1ID, point2ID, dist >
                 */

                for (GPSTrack i : iterable) {

                    //HashMap<Integer, List<GPSTrack>> elment = new HashMap<Integer, List<GPSTrack>>();

                    double dist = MAX_VALUE;

                    for (GPSTrack j : iterable) {
//                        if (i.isRowOrColumn() == true && j.isRowOrColumn() == false && j.getUid() == 1000) {
//
//                            double temp = getDistance(i.getLatitude(), i.getLongitude(), j.getLatitude(), j.getLongitude());
//
//                            if (temp < dist) {
//                                dist = temp;
//                                Tuple5<Integer, Integer, Integer, Integer, Double> element = new Tuple5(i.getUid(), j.getUid(), i.getId(), j.getId(), dist);
//                                collector.collect(element);
//                            }
//                        }
                        if (i.isRowOrColumn() == true && j.isRowOrColumn() == false) {

                            double temp = getDistance(i.getLatitude(), i.getLongitude(), j.getLatitude(), j.getLongitude());
                            /**
                             *
                             * 降负载
                             **/
                            if (temp < dist) {
                                dist = temp;
                                Tuple5<Integer, Integer, Integer, Integer, Double> element = new Tuple5(i.getUid(), j.getUid(), i.getId(), j.getId(), dist);
                                collector.collect(element);
                            }
                        }
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(windowTime))
                .apply(new WindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple3<Integer, Integer, Double>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> iterable, Collector<Tuple3<Integer, Integer, Double>> collector) throws Exception {
                        //System.out.println(tuple.getField(0).toString());
                        /**
                         * 备用
                         */
                       // Stream<Tuple5<Integer, Integer, Integer, Integer, Double>> tuplestream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable.iterator(),Spliterator.SORTED), false);
                       // tuplestream.distinct().collect(Collectors.groupingBy());


                        /**
                         * @key: T2ID
                         * @value: Map<Integer, List<Double>> i.e.,PoID, distList
                         */
                        Map<Integer, Map<Integer, List<Double>>> map = new HashMap<>();
                        iterable.forEach(t -> {
                                    if (map.containsKey(t.f1)){
                                        Map<Integer,List<Double>> maptemp =map.get(t.f1);
                                        if(maptemp.containsKey(t.f2)){
                                            List<Double> listtemp = maptemp.get(t.f2);
                                                listtemp.add(t.f4);
                                            maptemp.replace(t.f2,listtemp);

                                        }else{
                                            List<Double> list =new ArrayList<>();
                                            list.add(t.f4);
                                            maptemp.put(t.f2,list);
                                        }


                                        map.replace(t.f1,maptemp);
                                    }else{
                                        Map<Integer,List<Double>> maptemp =new HashMap<Integer,List<Double>>();
                                        List<Double> list =new ArrayList<>();
                                        list.add(t.f4);
                                        maptemp.put(t.f2,list);
                                        map.put(t.f1,maptemp);
                                    }
                                }
                        );
                        /**
                         *
                         */
                        map.forEach((id,mapp)->{
                            /**
                             * maptemp存了最终结果的距离
                             */
                            Map<String,Double> maptemp =new HashMap<>();
                            maptemp.put("max",Double.MIN_VALUE);
                            mapp.forEach((pid,list)->{
                                /**
                                 * min：得到该点到另一条轨迹的min
                                 */
                                Double min = list.stream().min((o1, o2) -> o2.compareTo(o1)).get();
                                if(maptemp.get("max") < min)
                                    maptemp.replace("max",min);
                            });
                            collector.collect(new Tuple3<Integer,Integer,Double>(tuple.getField(0),id,maptemp.get("max")));
                        });
                    }
                }).print().setParallelism(1);


//            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> iterable, Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
//                iterable.forEach(t->
//                {
//                    collector.collect(t);
//                });
//            }
//        }).keyBy(2)
//                .timeWindow(Time.seconds(windowTime)).minBy(4)
//                .timeWindowAll(Time.seconds(windowTime)).process(new ProcessAllWindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple5<Integer, Integer, Integer, Integer, Double>, TimeWindow>() {
//            @Override
//            public void process(Context context, Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> iterable, Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
//                iterable.forEach(t->
//                {
//                    collector.collect(t);
//                });
//            }
//        })
//                .keyBy(0)
//                .timeWindow(Time.seconds(windowTime))
//                .apply(new WindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow>() {
//            @Override
//            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> iterable, Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
//                iterable.forEach(t->{
//                    collector.collect(t);
//                });
//            }
//        })
//                .keyBy(1)
//                .timeWindow(Time.seconds(windowTime)).apply(new WindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple, TimeWindow>() {
//            @Override
//            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> iterable, Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
//                double flag =0;
//                Tuple5<Integer, Integer, Integer, Integer, Double> t = null;
//                for (Tuple5<Integer, Integer, Integer, Integer, Double> i :iterable){
//                    if (i.f4>flag){
//                        flag=i.f4;
//                        t=i;
//                    }
//                }
//                System.out.println(tuple.getField(0).toString());
//                collector.collect(t);
//            }
//        })
//                /*.maxBy(4)*/


//                .keyBy(String.valueOf(new KeySelector<Tuple3<GPSTrack, GPSTrack, Double>, Integer>() {
//            @Override
//            public Integer getKey(Tuple3<GPSTrack, GPSTrack, Double> element) throws Exception {
//                return element.f0.getUid();
//            }
//        })).keyBy(String.valueOf(new KeySelector<Tuple3<GPSTrack, GPSTrack, Double>, Integer>() {
//            @Override
//            public Integer getKey(Tuple3<GPSTrack, GPSTrack, Double> element) throws Exception {
//                return element.f1.getUid();
//            }
//        }))
  /*      streamInput.timeWindowAll(Time.hours(1))
                .process(new ProcessAllWindowFunction<Integer, Tuple3<Integer, Integer, GPSTrack>, TimeWindow>() {
                    int windowId = 0;

                    @Override
                    public void process(Context context, Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, GPSTrack>> out) throws Exception {
                        int columId = 0;
                        elements.forEach(value -> {
                            out.collect(new Tuple3<>(windowId, columId % 3, value));
                            out.collect(new Tuple3<>(windowId, columId % 3 + 3, value));
                            out.collect(new Tuple3<>(windowId, columId % 3 + 6, value));
                            columId++;
                        });
                        windowId++;
                    }
                });

        DataStream<Tuple3<Integer, Integer, GPSTrack>> stream2 = env.readTextFile("sort200_2.txt")
                .timeWindowAll(Time.hours(1)).process(new ProcessAllWindowFunction<GPSTrack, Tuple3<Integer, Integer, GPSTrack>, TimeWindow>() {
                    int windowId = 0;

                    @Override
                    public void process(Context context, Iterable<GPSTrack> elements, Collector<Tuple3<Integer, Integer, GPSTrack>> out) throws Exception {
                        int rowId = 0;
                        elements.forEach(value -> {
                            out.collect(new Tuple3<>(windowId, (rowId % 3) * 3 + 3, value));
                            out.collect(new Tuple3<>(windowId, (rowId % 3) * 3 + 1, value));
                            out.collect(new Tuple3<>(windowId, (rowId % 3) * 3 + 2, value));
                            rowId++;
                        });
                        windowId++;
                    }
                });

        stream1.union(stream2)
                .partitionCustom(new Partitioner<Tuple3<Integer, Integer, GPSTrack>>() {
                    @Override
                    public int partition(Tuple3<Integer, Integer, GPSTrack> integerIntegerGPSTrackTuple3, int i) {
                        return integerIntegerGPSTrackTuple3.f1;
                    }
                }, new KeySelector<Tuple3<Integer, Integer, GPSTrack>, Tuple3<Integer, Integer, GPSTrack> >() {
                    @Override
                    public Tuple3<Integer, Integer, GPSTrack>  getKey(Tuple3<Integer, Integer, GPSTrack> integerIntegerGPSTrackTuple3) throws Exception {
                        return integerIntegerGPSTrackTuple3 ;
                    }
                }).flatMap(new FlatMapFunction<Tuple3<Integer, Integer, GPSTrack>, Object>() {

            @Override
            public void flatMap(Tuple3<Integer, Integer, GPSTrack> elements, Collector<Object> out) throws Exception {
                out.collect(elements.f2);
            }
        })*/
//
//        stream.flatMap(new FlatMapFunction<String, GPSTrack>() {
//            @Override
//            public void flatMap(String s, Collector<GPSTrack> collector) throws Exception {
//                collector.collect(new GPSTrack(s));
//                collector.collect(new GPSTrack(s));
//            }
//        }).partitionCustom(new Partitioner<GPSTrack>() {
//            @Override
//            public Tuple2<Integer, Integer> partition(GPSTrack value, int numPartitions) {
//                return new Tuple2(1 % numPartitions, 2 % numPartitions);
//            }
//        }, new KeySelector<GPSTrack, GPSTrack>() {
//            @Override
//            public GPSTrack getKey(GPSTrack value) throws Exception {
//                return value;
//            }
//        })


        /**
         * TODO: Modify keyBy()
         */
//        DataStream streama = stream.map(new Map1())
//                .assignTimestampsAndWatermarks(new WaterMark1())
//                .timeWindowAll(Time.hours(10000))
//                .apply(new MatrixAlgorithm())
//                .keyBy(0).process(new KeyedProcessFunction<Tuple, Tuple5<Integer, Integer, Integer, GPSTrack, GPSTrack>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>>() {
//                    @Override
//                    public void processElement(Tuple5<Integer, Integer, Integer, GPSTrack, GPSTrack> integerGPSTrackGPSTrackTuple5, Context context, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> collector) throws Exception {
//                        /**
//                         *  Calculate distance of two points
//                         *  IN: tuple5< tupleId, index_i, index_j, Point1, Point2 >
//                         *  OUT: tuple6< tupleId, index_i, index_j, TrajectoryId1, TrajectoryId2, dist >
//                         */
//                        collector.collect(new Tuple6(integerGPSTrackGPSTrackTuple5.f0,
//                                integerGPSTrackGPSTrackTuple5.f1,
//                                integerGPSTrackGPSTrackTuple5.f2,
//                                integerGPSTrackGPSTrackTuple5.f3.getUid(),
//                                integerGPSTrackGPSTrackTuple5.f4.getUid(),
//                                getDistance(integerGPSTrackGPSTrackTuple5.f3.getLatitude(),
//                                        integerGPSTrackGPSTrackTuple5.f3.getLongitude(),
//                                        integerGPSTrackGPSTrackTuple5.f4.getLatitude(),
//                                        integerGPSTrackGPSTrackTuple5.f4.getLongitude())));
//
//                    }
//                });
        //SingleOutputStreamOperator max1 =

        // keyBy(3).keyBy(4).keyBy(1).min(5).keyBy(4).max(5).print();
        //SingleOutputStreamOperator max2 = streama.keyBy(4).keyBy(3).keyBy(1).min(5).keyBy(3).max(5);
        //max1.union(max2).print();
        env.execute();

    }


    /**
     * Calculate distance of two points represented by lat/lng
     *
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





