package org.apache.flink.alphafish;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MatrixAlgorithm implements AllWindowFunction<GPSTrack, Tuple5<Integer, Integer, Integer, GPSTrack, GPSTrack>, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<GPSTrack> iterable, Collector<Tuple5<Integer, Integer, Integer, GPSTrack, GPSTrack>> collector) throws Exception {

        Map<Integer, List<GPSTrack>> res = new HashMap<Integer, List<GPSTrack>>();

        iterable.forEach(g -> {
            if (res.containsKey(g.getUid())) {
                List<GPSTrack> gpsTrackList = res.get(g.getUid());
                gpsTrackList.add(g);
                res.put(g.getUid(), gpsTrackList);
            } else {
                List<GPSTrack> gpsTracks = new ArrayList<GPSTrack>();
                gpsTracks.add(g);
                res.put(g.getUid(), gpsTracks);
            }
        });

        List<GPSTrack> t1 = res.get(1104);
        List<GPSTrack> t2 = res.get(1195);
        List<GPSTrack> t3 = res.get(1305);
        List<GPSTrack> t4 = res.get(1292);

        int count = 0;
        int i = 1;
        int j = 1;
        for (GPSTrack p1 : t1) {
            for (GPSTrack p3 : t3) {
                /**
                 * tup5 : tupleId, index_i, index_j, GPSTrack, GPSTrack
                 * row: index_i
                 * colum: index_j
                 */
                Tuple5 temp = new Tuple5(count++, i, j, p1, p3);
                i++;
                collector.collect(temp);
            }
            i = 1;
            j++;
        }

        i = 1;
        j = 1;
        for (GPSTrack p1 : t1) {
            for (GPSTrack p4 : t4) {
                /**
                 * tup5 : tupleId, index_i, index_j, GPSTrack, GPSTrack
                 * row: index_i
                 * colum: index_j
                 */
                Tuple5 temp = new Tuple5(count++, i, j, p1, p4);
                i++;
                collector.collect(temp);
            }
            i = 1;
            j++;
        }


        i = 1;
        j = 1;
        for (GPSTrack p2 : t2) {
            for (GPSTrack p3 : t3) {
                /**
                 * tup5 : tupleId, index_i, index_j, GPSTrack, GPSTrack
                 * row: index_i
                 * colum: index_j
                 */
                Tuple5 temp = new Tuple5(count++, i, j, p2, p3);
                i++;
                collector.collect(temp);
            }
            i = 1;
            j++;
        }

        i = 1;
        j = 1;
        for (GPSTrack p2 : t2) {
            for (GPSTrack p4 : t4) {
                /**
                 * tup5 : tupleId, index_i, index_j, GPSTrack, GPSTrack
                 * row: index_i
                 * colum: index_j
                 */
                Tuple5 temp = new Tuple5(count++, i, j, p2, p4);
                i++;
                collector.collect(temp);
            }
            i = 1;
            j++;
        }


    }
}
