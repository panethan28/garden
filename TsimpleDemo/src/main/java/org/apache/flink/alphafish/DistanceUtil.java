package org.apache.flink.alphafish;

import java.awt.geom.GeneralPath;
import java.awt.geom.Point2D;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author Gavin
 *
 */
public final class DistanceUtil {

	/**
	 * 地球半径,单位 m
	 */
	private static final double EARTH_RADIUS = 6378.137 * 1000;

	/**
	 * 根据经纬度，计算两点间的距离
	 *
	 * @param longitude1
	 *            第一个点的经度
	 * @param latitude1
	 *            第一个点的纬度
	 * @param longitude2
	 *            第二个点的经度
	 * @param latitude2
	 *            第二个点的纬度
	 * @return 返回距离 单位千米
	 */
	public static double getDistance(double longitude1, double latitude1,
			double longitude2, double latitude2) {
		// 纬度
		double lat1 = Math.toRadians(latitude1);
		double lat2 = Math.toRadians(latitude2);
		// 经度
		double lng1 = Math.toRadians(longitude1);
		double lng2 = Math.toRadians(longitude2);
		// 纬度之差
		double a = lat1 - lat2;
		// 经度之差
		double b = lng1 - lng2;
		// 计算两点距离的公式
		double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2)
				+ Math.cos(lat1) * Math.cos(lat2)
				* Math.pow(Math.sin(b / 2), 2)));
		// 弧长乘地球半径, 返回单位: 千米
		s = s * EARTH_RADIUS;
		return s;
	}

	/**
	 * 判断一个点是否在圆形区域内
	 */
	public static boolean isInCircle(double lng1, double lat1, double lng2,
			double lat2, String radius) {
		double distance = getDistance(lat1, lng1, lat2, lng2);
		double r = Double.parseDouble(radius);
		if (distance > r) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * 
	 * @param g1
	 *            GPSTrack
	 * @param g2
	 *            GPSTrack
	 * @return 经纬度间的距离 精确到米
	 */
	public static double getDistance(GPSTrack g1, GPSTrack g2) {
		return getDistance(g1.getLongitude(), g1.getLatitude(),
				g2.getLongitude(), g2.getLatitude());
	}
	
//	public static double getDistance(SpatialPoint sp1, SpatialPoint sp2) {
//		return getDistance(sp1.getLon(), sp1.getLat(),
//				sp2.getLon(), sp2.getLat());
//	}
	/**
	 * 
	 * @param g
	 * @param sp
	 * @return 
	 */
//	public static double getDistance(List<GPSTrack> g, SpatialPoint sp) {
//		double minDistance = Double.MAX_VALUE;
//		for (GPSTrack gpsTrack : g) {
//			double distanceTemp=getDistance(gpsTrack.getLongitude(),gpsTrack.getLatitude(),sp.getLon(),sp.getLat());
//			if(minDistance > distanceTemp){
//				minDistance = distanceTemp;
//			}
//		}
//		return minDistance;
//	}
	public  double getDistance2(double longitude1, double latitude1,
			double longitude2, double latitude2) {

		double latDistance = Math.toRadians(longitude1 - longitude2);
		double lngDistance = Math.toRadians(latitude1 - latitude2);

		double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
				+ Math.cos(Math.toRadians(longitude1))
				* Math.cos(Math.toRadians(longitude2))
				* Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2);

		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		return c * EARTH_RADIUS;
	}

	/**
	 * 
	 * @param t1
	 *            轨迹1
	 * @param t2
	 *            轨迹2
	 * @param distance
	 *            米
	 * @return min diatance is less than distance?
	 */
//	public static boolean isNeighbour(Trajectory t1, Trajectory t2,
//			double distance) {
//		for (GPSTrack g1 : t1.getList()) {
//			for (GPSTrack g2 : t2.getList()) {
//				if (getDistance(g1, g2) > distance)
//					return true;
//			}
//		}
//		return false;
//	}

	/**
	 * 判断是否在多边形区域内
	 * 
	 * @param pointLon
	 *            要判断的点的纵坐标
	 * @param pointLat
	 *            要判断的点的横坐标
	 * @param lon
	 *            区域各顶点的纵坐标数组
	 * @param lat
	 *            区域各顶点的横坐标数组
	 * @return
	 */
	public static boolean isInPolygon(double pointLon, double pointLat,
			double[] lon, double[] lat) {
		// 将要判断的横纵坐标组成一个点
		Point2D.Double point = new Point2D.Double(pointLon, pointLat);
		// 将区域各顶点的横纵坐标放到一个点集合里面
		List<Point2D.Double> pointList = new ArrayList<Point2D.Double>();
		double polygonPoint_x = 0.0, polygonPoint_y = 0.0;
		for (int i = 0; i < lon.length; i++) {
			polygonPoint_x = lon[i];
			polygonPoint_y = lat[i];
			Point2D.Double polygonPoint = new Point2D.Double(polygonPoint_x,
					polygonPoint_y);
			pointList.add(polygonPoint);
		}
		return check(point, pointList);
	}

	/**
	 * 一个点是否在多边形内
	 * 
	 * @param point
	 *            要判断的点的横纵坐标
	 * @param polygon
	 *            组成的顶点坐标集合
	 * @return
	 */
	private static boolean check(Point2D.Double point,
			List<Point2D.Double> polygon) {
		GeneralPath peneralPath = new GeneralPath();

		Point2D.Double first = polygon.get(0);
		// 通过移动到指定坐标（以双精度指定），将一个点添加到路径中
		peneralPath.moveTo(first.x, first.y);
		polygon.remove(0);
		for (Point2D.Double d : polygon) {
			// 通过绘制一条从当前坐标到新指定坐标（以双精度指定）的直线，将一个点添加到路径中。
			peneralPath.lineTo(d.x, d.y);
		}
		// 将几何多边形封闭
		peneralPath.lineTo(first.x, first.y);
		peneralPath.closePath();
		// 测试指定的 Point2D 是否在 Shape 的边界内。
		return peneralPath.contains(point);
	}

	/**
	 * 判断估计 T 是否在 peneralPath 区域内
	 * 
	 * @param peneralPath
	 * @param t
	 * @return
	 */
//	public static boolean isInTrajectoryRegion(GeneralPath peneralPath, Trajectory t) {
//		Point2D.Double point;
//		for (GPSTrack g : t.getList()) {
//			point = new Point2D.Double(g.getLongitude(), g.getLatitude());
//			if (peneralPath.contains(point))
//				return true;
//		}
//		return false;
//	}
	/**
	 * 估计t的点在闭合区域peneralPath的个数
	 * @param peneralPath
	 * @param t
	 * @return
	 */
//	public static int pointsInTrajectoryRegion(GeneralPath peneralPath, Trajectory t) {
//		Point2D.Double point;
//		int count=0;
//		for (GPSTrack g : t.getList()) {
//			point = new Point2D.Double(g.getLongitude(), g.getLatitude());
//			if (peneralPath.contains(point))
//				count=0;
//		}
//		return count;
//	}
	/**
	 * 创建一条轨迹的闭合区域
	 * 
	 * @param t
	 * @param r
	 * @return
	 */
//	public static GeneralPath createGeneralPath(Trajectory t, double r) {
//		GeneralPath peneralPath = new GeneralPath();
//		List<GPSTrack> list = t.getList();
//		int k=list.size();
//		double polygonPoint_x = 0.0, polygonPoint_y = 0.0;
//		Point2D.Double first = ConvertDistanceToLogLat(r, list.get(0)
//				.getLongitude(), list.get(0).getLatitude(), 90);
//		peneralPath.moveTo(first.x, first.y);
//		if(k>1)
//			for (int i = 1; i < list.size(); i++) {
//				polygonPoint_x = list.get(i).getLongitude();
//				polygonPoint_y = list.get(i).getLatitude();
//				Point2D.Double polygonPoint = ConvertDistanceToLogLat(r,
//						polygonPoint_x, polygonPoint_y, 90);
//				peneralPath.lineTo(polygonPoint.x, polygonPoint.y);
//			}
//		Point2D.Double polygonPointconer1 = ConvertDistanceToLogLat(r, list
//				.get(list.size() - 1).getLongitude(), list.get(list.size() - 1)
//				.getLatitude(), 0);
//		Point2D.Double polygonPointconer2 = ConvertDistanceToLogLat(r, list
//				.get(list.size() - 1).getLongitude(), list.get(list.size() - 1)
//				.getLatitude(), -90);
//		peneralPath.quadTo(polygonPointconer1.x, polygonPointconer1.y,
//				polygonPointconer2.x, polygonPointconer2.y);
//		if(k>1)
//			for (int i = list.size() - 2; i >= 0; i--) {
//				polygonPoint_x = list.get(i).getLongitude();
//				polygonPoint_y = list.get(i).getLatitude();
//				Point2D.Double polygonPoint = ConvertDistanceToLogLat(r,
//						polygonPoint_x, polygonPoint_y, -90);
//				peneralPath.lineTo(polygonPoint.x, polygonPoint.y);
//			}
//		Point2D.Double last = ConvertDistanceToLogLat(r, list.get(0)
//				.getLongitude(), list.get(0).getLatitude(), -180);
//		peneralPath.quadTo(last.x, last.y, first.x, first.y);
//		peneralPath.closePath();
//		return peneralPath;
//	}

	/**
	 * 
	 * @param distance
	 * @param lng1
	 * @param lat1
	 * @param angle
	 * @return 给定方位 距离 和一个点的经纬度，求另个点的经纬度
	 */
	public static Point2D.Double ConvertDistanceToLogLat(double distance,
			double lng1, double lat1, double angle) {
		double lon = lng1 + (distance * Math.sin(angle * Math.PI / 180))
				/ (111 * Math.cos(lat1 * Math.PI / 180));// 将距离转换成经度的计算公式
		double lat = lat1 + (distance * Math.cos(angle * Math.PI / 180)) / 111;// 将距离转换成纬度的计算公式
		Point2D.Double point = new Point2D.Double(lon, lat);
		return point;
	}
	/**
	 * 区域中心点判别
	 * @param pid
	 * @return
	 */
//	public static SpatialPoint getCenterPoint(Integer pid){
//		switch (pid) {
//		case 1:
//			return new SpatialPoint(39.7, 166.2);
//		case 2:
//			return new SpatialPoint(40.1, 166.6);
//		case 3:
//			return new SpatialPoint(40.1, 166.2);
//		case 4:
//			return new SpatialPoint(39.7, 166.6);
//		default:
//			return null;
//		}
//	}
	/**
	 * 获取区域内平均坐标以及个数
	 * @param list 轨迹集合
	 * @return
	 */
//	public static Tuple2<Integer,SpatialPoint> getAvgCoordinate(List<GPSTrack> list){
//		double lon=0;
//		double lat=0;
//		int i=0;
//		for (GPSTrack gpsTrack : list) {
//			lon+=gpsTrack.getLongitude();
//			lat+=gpsTrack.getLongitude();
//			i++;
//		}
//		return new Tuple2<>(i,new SpatialPoint(lon/i,lat/i));
//	}
	
//	public static void Test(String[] args) throws ParseException {
//		double d = getDistance(39.5, 116, 40.3, 116.8);
//		//double d1 = this.getDistance2(116, 39.5, 116.8, 40.3);
//		Point2D.Double point = ConvertDistanceToLogLat(20, 39, 116, -90);
//		System.out.println(point.getX() + "---" + point.getY());
//		System.out.println(d);
//		//System.out.println(d1);
//		Trajectories ts=new Trajectories();
//		ts.addGPSTrack(new GPSTrack("157,2008-02-02 13:37:24,116.32294,39.89857"));
//		ts.addGPSTrack(new GPSTrack("157,2008-02-02 13:37:24,116.32294,39.89857"));
//		ts.addGPSTrack(new GPSTrack("157,2008-02-02 13:37:24,116.32294,39.89857"));
//		GeneralPath p = createGeneralPath(ts.getTrajectoryByTid(157),20);
//		System.out.println(isInTrajectoryRegion(p,ts.getTrajectoryByTid(157)));
//	}
//	@Test
//	public void DisTest() {
//		double d1 = this.getDistance2(116, 39.5, 116, 40.3);
//		System.out.println(d1);
//	}
}
