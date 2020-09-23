package org.apache.flink.alphafish;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;





/**
 * @author Gavin
 * hausdorff distance 时空
 */
public class Hausdorff {
	private List<GPSTrack> t1;
	private List<GPSTrack> t;
//	List<SpatialPoint> list;
//	List<SpatialPoint> spabs;
	public Hausdorff(List<GPSTrack> t1, List<GPSTrack> t2) {
		super();
	}
	
//	public static List<Integer> getTopKhausdorffDistance(List<SpatialPoint> lst, List<TrajectorySummary> list,Integer k,Integer id) {
//		List<Integer> lstres =new ArrayList<Integer>();
//    	Map<Integer,Double> map = new java.util.HashMap<Integer, Double>();
//    	for (TrajectorySummary t : list) {
//    		if(id!=t.getTid()){
//    			Double d= gethausdorffDistance(lst, t.getList());
//        		map.put(t.getTid(), d);
//    		}
//
//		}
//    	lstres = CompareUtil.getTop_K(map,k);
//    	return lstres;
//	}
	
//	public static double gethausdorffDistance(List<SpatialPoint> list, List<SpatialPoint> spabs) {
//		List<Double> list1=new LinkedList<Double>();
//		List<Double> list2=new LinkedList<Double>();
//		for (SpatialPoint g1 : list) {
//			double mindis = Integer.MAX_VALUE ;
//			for (SpatialPoint g2 : spabs) {
//				double distemp = DistanceUtil.getDistance(g1,g2);
//				if(distemp < mindis)
//					mindis=distemp;
//			}
//			list1.add(mindis);
//		}
//		for (SpatialPoint g1 : spabs) {
//			double mindis = Integer.MAX_VALUE ;
//			for (SpatialPoint g2 : list) {
//				double distemp = DistanceUtil.getDistance(g1,g2);
//				if(distemp < mindis)
//					mindis=distemp;
//			}
//			list2.add(mindis);
//		}
//		Collections.sort(list1);
//		Collections.sort(list2);
//		return list1.get(list1.size()-1)>=list2.get(list2.size()-1)?list1.get(list1.size()-1):list2.get(list2.size()-1);
//	}
	
	public static double  hausdorffDistance(List<GPSTrack> t1,List<GPSTrack> t2){
		//long start= System.currentTimeMillis();
		List<Double> list1=new ArrayList<Double>();
		List<Double> list2=new ArrayList<Double>();
		for (GPSTrack g1 : t1) {
			double mindis = Integer.MAX_VALUE ;
			for (GPSTrack g2 : t2) {
				double distemp = DistanceUtil.getDistance(g1,g2);
				if(distemp < mindis)
					mindis=distemp;
			}
			list1.add(mindis);
		}
		for (GPSTrack g1 : t2) {
			double mindis = Integer.MAX_VALUE ;
			for (GPSTrack g2 : t1) {
				double distemp = DistanceUtil.getDistance(g1,g2);
				if(distemp < mindis)
					mindis=distemp;
			}
			list2.add(mindis);
		}
		Collections.sort(list1);
		Collections.sort(list2);
		double d= list1.get(list1.size()-1)>=list2.get(list2.size()-1)?list1.get(list1.size()-1):list2.get(list2.size()-1);
		return d;
	}
	
	public Hausdorff() {
		super();
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		List<GPSTrack> l1=new ArrayList<GPSTrack>();
		List<GPSTrack> l2=new ArrayList<GPSTrack>();
		l1.add(new GPSTrack(15.0,20.0));
		l1.add(new GPSTrack(10.0,38.0));
		l2.add(new GPSTrack(20.0,25.0));
		l2.add(new GPSTrack(30.0,20.0));
		Hausdorff h=new Hausdorff(l1, l2);
		System.out.println(h.hausdorffDistance(l1,l2));
	}

//	public static double gethausdorffDistance(LinkedList<Point> points,
//			LinkedList<Point> points2) {
//		List<Double> list1=new ArrayList<Double>();
//		List<Double> list2=new ArrayList<Double>();
//		for (Point point : points) {
//			double mindis = Integer.MAX_VALUE ;
//			for (Point point2 : points2) {
//				double distemp = point.distancePoint(point2);
//				if(distemp < mindis)
//					mindis=distemp;
//			}
//			list1.add(mindis);
//		}
//		for (Point point : points2) {
//			double mindis = Integer.MAX_VALUE ;
//			for (Point point2 : points) {
//				double distemp = point.distancePoint(point2);
//				if(distemp < mindis)
//					mindis=distemp;
//			}
//			list2.add(mindis);
//		}
//		double mindis1=Collections.min(list1);
//		double mindis2=Collections.min(list2);
//		return Math.max(mindis1, mindis2);
//	}
}
