package org.apache.flink.alphafish;

import org.apache.flink.api.common.functions.MapFunction;

public class Map1 implements MapFunction<String,GPSTrack> {
    @Override
    public GPSTrack map(String value) throws Exception {
        return new GPSTrack(value);
    }
}
