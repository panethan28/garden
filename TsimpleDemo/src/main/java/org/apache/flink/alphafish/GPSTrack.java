package org.apache.flink.alphafish;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

public class GPSTrack {
    static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /**
     * think 0-1
     */
    private static Long i=1L;
    private int id; //注册即分配的id
    private int uid; //用户id
    private Integer pid; //进程id，一开始为0
    private Long timeStamp;
    private List<Integer> intoSID; //进入的槽位
    private boolean rowOrColumn;//横向还是纵向



    public boolean isRowOrColumn() {
        return rowOrColumn;
    }

    public void setRowOrColumn(boolean rowOrColumn) {
        this.rowOrColumn = rowOrColumn;
    }

    public List<Integer> getIntoSID() {
        return intoSID;
    }

    public void setIntoSID(List<Integer> intoSID) {
        this.intoSID = intoSID;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    private Long intime;
    private Double longitude;
    private Double latitude;


    public Long getIntime() {
        return intime;
    }

    public void setIntime(Long intime) {
        this.intime = intime;
    }

    public GPSTrack(String s) throws ParseException{
        String[] tmp=s.split(",");
        this.id=i.intValue();
        i++;
        uid=Integer.parseInt(tmp[0]);
        timeStamp=TimeStamp.dateToStamp(tmp[1]);
        longitude=Double.parseDouble(tmp[2]);
        latitude=Double.parseDouble(tmp[3]);
    }
    public GPSTrack(){

    }

    public GPSTrack(Double longitude, Double latitude) {
        super();
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return id + " " + Integer.toString(uid)+" "+timeStamp.toString()+" "+longitude.toString()+" "+latitude.toString()+" "+pid;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }
}

