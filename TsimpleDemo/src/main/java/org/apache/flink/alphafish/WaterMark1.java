package org.apache.flink.alphafish;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class WaterMark1 implements AssignerWithPeriodicWatermarks<GPSTrack> {
    private final long outoforderness = 10;
    private long currentwatermark = 0;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentwatermark - outoforderness);
    }

    @Override
    public long extractTimestamp(GPSTrack element, long l) {
        currentwatermark = Math.max(element.getTimeStamp(), currentwatermark);
        return currentwatermark;
    }
}
