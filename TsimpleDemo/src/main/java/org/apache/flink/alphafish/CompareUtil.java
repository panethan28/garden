package org.apache.flink.alphafish;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;



public class CompareUtil {
	/**
	 * 
	 * @param map
	 * @param k
	 * @return
	 */
	public static List<Integer> getTop_K(Map<Integer, Double> map,int k) {
		List<Integer> list=new ArrayList<Integer>();
		Map<Integer, Double> maptemp=new HashMap<Integer, Double>();
		maptemp.putAll(map);
		Map<Integer, Double> ascmap= sortMapByValueASC(maptemp);
		Iterator<Integer> set = ascmap.keySet().iterator();
		for (int i = 0; i < k&&set.hasNext(); i++) {
			list.add(set.next());
		}
		return list;
	}
	public static  Map<Integer,List<Integer>> mapSort1(Map<Integer,Map<Integer,Double>> map,int k){
		Iterator<Entry<Integer, Map<Integer, Double>>> entries = map.entrySet().iterator();
		Map<Integer,List<Integer>> resultMap=new HashMap<Integer, List<Integer>>();
		while(entries.hasNext()){
			Entry<Integer, Map<Integer, Double>> entry=entries.next();
			Map<Integer, Double> descmap = sortMapByValueDescending(entry.getValue());
			Iterator<Entry<Integer, Double>> entries1 = descmap.entrySet().iterator();
			int i=0;
			List<Integer> list =new ArrayList<Integer>();
			while (i<k&&entries1.hasNext()) {
				list.add(entries1.next().getKey());
				i++;
			}
			resultMap.put(entry.getKey(),list);
		}
		return resultMap;
	}
	
	public static  Map<Integer,List<Integer>> mapSortASC(Map<Integer,Map<Integer,Double>> map,int k){
		Iterator<Entry<Integer, Map<Integer, Double>>> entries = map.entrySet().iterator();
		Map<Integer,List<Integer>> resultMap=new HashMap<Integer, List<Integer>>();
		while(entries.hasNext()){
			Entry<Integer, Map<Integer, Double>> entry=entries.next();
			Map<Integer, Double> descmap = sortMapByValueASC(entry.getValue());
			Iterator<Entry<Integer, Double>> entries1 = descmap.entrySet().iterator();
			int i=0;
			List<Integer> list =new ArrayList<Integer>();
			if(entry.getKey()==88)
			{
			
			
		    	System.out.println( descmap.toString());
			}	
			while (i<k&&entries1.hasNext()) {
				list.add(entries1.next().getKey());
				i++;
			}
						
			resultMap.put(entry.getKey(),list);
		}
		return resultMap;
	}
	public static Tuple2<Integer,Map<Integer, List<Integer>>> mapSortDoubleASC2(Map<Integer,Map<Integer,Double>> map,int k) {
		
		Iterator<Entry<Integer, Map<Integer, Double>>> entries = map.entrySet().iterator();
		Map<Integer,List<Integer>> resultMap=new HashMap<Integer, List<Integer>>();
		int haskcount=0;
		while(entries.hasNext()){
			Entry<Integer, Map<Integer, Double>> entry=entries.next();
			Map<Integer, Double> asccmap = sortMapByValueASC(entry.getValue());
			//System.out.println(asccmap.toString());
			Iterator<Entry<Integer, Double>> entries1 = asccmap.entrySet().iterator();
			List<Integer> list =new ArrayList<Integer>();
			while (entries1.hasNext()) {
				list.add(entries1.next().getKey());
			}
			//System.out.println("list===>"+list);
			if(list.size()>k)
				{
					haskcount++;
					list.subList(k,list.size()).clear();
				}
			resultMap.put(entry.getKey(),list);
		}
		
		return new Tuple2<Integer, Map<Integer, List<Integer>>>(haskcount,resultMap);
	}
	public static Map<Integer, List<Integer>> mapSortInteger(Map<Integer, Map<Integer, Integer>> map, int k) {
		Iterator<Entry<Integer, Map<Integer, Integer>>> entries = map
				.entrySet().iterator();
		Map<Integer, List<Integer>> resultMap = new HashMap<Integer, List<Integer>>();
		while (entries.hasNext()) {
			Entry<Integer, Map<Integer, Integer>> entry = entries.next();
			Map<Integer, Integer> descmap = sortMapByValueDescending(entry
					.getValue());
			Iterator<Entry<Integer, Integer>> entries1 = descmap.entrySet()
					.iterator();
			int i = 0;
			List<Integer> list = new ArrayList<Integer>();
			while (i < k && entries1.hasNext()) {
				list.add(entries1.next().getKey());
				i++;
			}
			resultMap.put(entry.getKey(), list);
		}
		return resultMap;
	}
public static Tuple2<Integer,Map<Integer, List<Integer>>> mapSortDoubleASC(Map<Map<Integer,Integer>,Double> map, int k) {
		
		Map<Integer, List<Integer>> resultMap = new HashMap<Integer, List<Integer>>();
		Map<Map<Integer,Integer>,Double> mapdes=sortMapByValueASC(map);
		Map<Integer, Integer> maptemp;
		//System.out.println(mapdes.toString());
		for (Map.Entry<Map<Integer, Integer>, Double> entry : mapdes.entrySet()) {		
			maptemp=entry.getKey();
			maptemp.forEach((key,value)->{
				if(resultMap.containsKey(key)){
					List<Integer> l=resultMap.get(key);
					l.add(value);
					resultMap.replace(key, l);
				}else{
					List<Integer> l=new ArrayList<Integer>();
					l.add(value);
					resultMap.put(key, l);
				}
			});
			
		}
		int haskcount=0;
		for (Entry<Integer, List<Integer>> entry : resultMap.entrySet()) {
			List<Integer> ll= entry.getValue();
			if(ll.size()>k){
				/*ll.subList(k,ll.size()).clear();
				resultMap.replace(entry.getKey(), ll);*/
				haskcount++;
			}
		}
		
		return new Tuple2<Integer, Map<Integer, List<Integer>>>(haskcount,resultMap);
	}


	public static Tuple2<Integer,Map<Integer, List<Integer>>> mapSortDouble(Map<Map<Integer,Integer>,Double> map, int k) {
		
		Map<Integer, List<Integer>> resultMap = new HashMap<Integer, List<Integer>>();
		Map<Map<Integer,Integer>,Double> mapdes=sortMapByValueDescending(map);
		System.out.println(mapdes.toString());
		for (Map.Entry<Map<Integer, Integer>, Double> entry : mapdes.entrySet()) {		
			Map<Integer, Integer> maptemp=entry.getKey();
			maptemp.forEach((key,value)->{
				if(resultMap.containsKey(key)){
					List<Integer> l=resultMap.get(key);
					l.add(value);
					resultMap.replace(key, l);
				}else{
					List<Integer> l=new ArrayList<Integer>();
					l.add(value);
					resultMap.put(key, l);
				}
			});
			
		}
		System.out.println(resultMap);
		int haskcount=0;
		for (Entry<Integer, List<Integer>> entry : resultMap.entrySet()) {
			List<Integer> ll= entry.getValue();
			if(ll.size()>k){
				ll.subList(k,ll.size()).clear();
				resultMap.replace(entry.getKey(), ll);
				haskcount++;
			}
		}
		
		return new Tuple2<Integer, Map<Integer, List<Integer>>>(haskcount,resultMap);
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortMapByValueDescending(
			Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				int compare = (o1.getValue()).compareTo(o2.getValue());
				return -compare;
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
	
	public static <K, V extends Comparable<? super V>> Map<K, V> sortMapByValueASC(
			Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			@Override
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				int compare = (o1.getValue()).compareTo(o2.getValue());
				return compare;
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
	
	public static double[] getTop1(double[][] a) {
		double[] min = new double[a.length];
		for (int i = 0; i < a.length; i++) {
			double m = a[i][0];
			for (int j = 1; j < a[i].length; j++) {
				if (a[i][j] < m)
					m = a[i][j];
			}
			min[i] = m;
		}
		return min;
	}

	public static double[][] getTop_K(double[][] dis, int k) {
		double top_k[][] = new double[dis.length][k];
		for (int i = 0; i < dis.length; i++) {
			double[] topk = new double[k];
			for (int j = 0; j < k; j++) {
				topk[i] = dis[i][j];
			}

			// 转换成最小堆
			MinHeap heap = new MinHeap(topk);

			// 从k开始，遍历data
			for (int m = k; m < dis.length; i++) {
				double root = heap.getRoot();

				// 当数据大于堆中最小的数（根节点）时，替换堆中的根节点，再转换成堆
				if (dis[i][m] > root) {
					heap.setRoot(dis[i][m]);
				}
			}

			for (int n = 0; n < topk.length; k++) {
				top_k[i][n] = topk[n];
			}
		}
		return top_k;
	}

	public static double[][] getLessThanThreshold(double[][] dis, double t) {
		double lt[][] = new double[dis.length][];
		for (int i = 0; i < dis.length; i++)
			for (int j = 0; j < dis[i].length; j++) {
				int k = 0;
				if (dis[i][j] < t)
					lt[i][k] = dis[i][j];
			}
		return lt;
	}

	public static void main(String[] args) {
		Map<Integer, Double> descmap = new HashMap<Integer, Double>();
		Map<Integer, Double> map = new HashMap<Integer, Double>();
		descmap.put(1, 2.1);
		descmap.put(2, 22.5);
		descmap.put(4, 8.1);
		map = sortMapByValueASC(descmap);
		for (Entry<Integer, Double> entry : map.entrySet())
			System.out.println("Key = " + entry.getKey() + ", Value = "
					+ entry.getValue());
		
	}

	public static List<Tuple3<Integer, Integer, Integer>> getTop_KSort(
			List<Tuple3<Integer, Integer, Integer>> list1, int k) {
		List<Tuple3<Integer, Integer, Integer>> list = new ArrayList<Tuple3<Integer, Integer, Integer>>();
		list1.sort(new Comparator<Tuple3<Integer, Integer, Integer>>() {

			@Override
			public int compare(Tuple3<Integer, Integer, Integer> o1,
					Tuple3<Integer, Integer, Integer> o2) {
				if (o1.f2 > o2.f2)
					return -1;
				if (o1.f2 < o2.f2)
					return -1;
				return 0;
			}
		});
		for (int i = 0; i < k; i++) {
			if (i < list1.size())
				list.add(list1.get(i));
		}
		return list;
	}

}
