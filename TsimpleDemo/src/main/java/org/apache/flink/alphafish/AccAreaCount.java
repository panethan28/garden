package org.apache.flink.alphafish;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AccAreaCount {
    public void GivenAreaSum(final double d1,final double d2,final double d3, final double d4) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        DataStreamSource<String> stream = env.readTextFile("sort200_2.txt");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
      /*  final GPSTrack g2 = new GPSTrack(g1.getLongitude(), g4.getLatitude());
        final GPSTrack g3 = new GPSTrack(g4.getLongitude(), g1.getLatitude());
        final List<GPSTrack> gpsTracks = new ArrayList<GPSTrack>();
        gpsTracks.add(g1);
        gpsTracks.add(g2);
        gpsTracks.add(g3);
        gpsTracks.add(g4);*/
        stream.map(new MapFunction<String, GPSTrack>() {

            public GPSTrack map(String value) throws Exception {
                // TODO Auto-generated method stub
                return new GPSTrack(value);
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<GPSTrack>() {
            private final long outoforderness = 3500;
            private long currentwatermark;

            public long extractTimestamp(GPSTrack element, long previousElementTimestamp) {
                // TODO Auto-generated method stub
                currentwatermark = Math.max(element.getTimeStamp(), currentwatermark);
                return element.getTimeStamp();
            }

            public Watermark getCurrentWatermark() {
                // TODO Auto-generated method stub
                return new Watermark(currentwatermark - outoforderness);
            }
        }).partitionCustom(new Partitioner<GPSTrack>() {

            public int partition(GPSTrack value, int numPartitions) {
                if (value.getLatitude() <= 39.9 && value.getLongitude() <= 116.4)
                    return 1 % numPartitions;
                if (value.getLatitude() > 39.9 && value.getLongitude() > 116.4)
                    return 2 % numPartitions;
                if (value.getLatitude() > 39.9 && value.getLongitude() <= 116.4)
                    return 3 % numPartitions;
                if (value.getLatitude() <= 39.9 && value.getLongitude() > 116.4)
                    return 4 % numPartitions;
                return 5;
            }
        }, new KeySelector<GPSTrack, GPSTrack>() {

            public GPSTrack getKey(GPSTrack value) throws Exception {
                // TODO Auto-generated method stub
                return value;
            }
        }).map(new RichMapFunction<GPSTrack, GPSTrack>() {

            int pid;

            @Override
            public void open(Configuration parameters) throws Exception {
                // TODO Auto-generated method stub
                super.open(parameters);
                pid = getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public GPSTrack map(GPSTrack arg0) throws Exception {
                // TODO Auto-generated method stub
                arg0.setPid(pid);
                return arg0;
            }
        }).keyBy(new KeySelector<GPSTrack, Integer>() {

            public Integer getKey(GPSTrack value) throws Exception {
                // TODO Auto-generated method stub
                return value.getPid();
            }
        }).timeWindow(Time.seconds(3000), Time.seconds(10)).apply(new WindowFunction<GPSTrack, Integer, Integer, TimeWindow>() {

            public void apply(Integer key, TimeWindow window, Iterable<GPSTrack> input, Collector<Integer> out)
                    throws Exception {


                int i = 0;
                Iterator<GPSTrack> points = input.iterator();
                while (points.hasNext()) {
                    GPSTrack gg = points.next();
                    if (key.intValue() == 0) {
                        if (gg.getLatitude() > d1 && gg.getLongitude() < d2)
                            i++;
                    } else if (key.intValue() == 1) {
                        if (gg.getLatitude() < d3 && gg.getLongitude() < d2)
                            i++;
                    } else if (key.intValue() == 2) {
                        if (gg.getLatitude() > d1 && gg.getLongitude() > d4)
                            i++;
                    } else {
                        if (gg.getLatitude() < d3 && gg.getLongitude() > d4)
                            i++;
                    }
                }

                System.out.println("当前窗口时间：" + window.getStart() + " -- " + window.getEnd() + ", 区域" + key + " ：" + i + "个点");
                out.collect(i);
            }
        });
        //stream.print();

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        GPSTrack g1 = new GPSTrack(116.2, 39.7);
        GPSTrack g2 = new GPSTrack(116.6, 40.1);
        AccAreaCount accAreaCount = new AccAreaCount();
        accAreaCount.GivenAreaSum(116.2, 39.7,116.6, 40.1);
    }


}