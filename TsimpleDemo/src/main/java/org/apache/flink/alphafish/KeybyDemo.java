/**
 *
 */
package org.apache.flink.alphafish;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author Gavin
 *
 */
public class KeybyDemo {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();
		env.setParallelism(4);
		ArrayList<Tuple5<Integer, Integer, Integer, Integer, Double>> broadData = new ArrayList<>();
		broadData.add(new Tuple5<>(1, 3, 1, 5, 20.0));
		broadData.add(new Tuple5<>(1, 3, 1, 6, 30.0));
		broadData.add(new Tuple5<>(1, 3, 2, 5, 21.0));
		broadData.add(new Tuple5<>(1, 3, 2, 6, 31.0));
		broadData.add(new Tuple5<>(1, 4, 1, 7, 15.0));
		broadData.add(new Tuple5<>(1, 4, 1, 8, 16.0));
		broadData.add(new Tuple5<>(1, 4, 2, 7, 17.0));
		broadData.add(new Tuple5<>(1, 4, 2, 8, 18.0));
		broadData.add(new Tuple5<>(2, 3, 3, 5, 19.0));
		broadData.add(new Tuple5<>(2, 3, 3, 6, 22.0));
		broadData.add(new Tuple5<>(2, 3, 4, 5, 23.0));
		broadData.add(new Tuple5<>(2, 3, 4, 6, 24.0));
		broadData.add(new Tuple5<>(2, 4, 3, 7, 25.0));
		broadData.add(new Tuple5<>(2, 4, 3, 8, 26.0));
		broadData.add(new Tuple5<>(2, 4, 4, 7, 27.0));
		broadData.add(new Tuple5<>(2, 4, 4, 8, 28.0));

		DataStreamSource<Tuple5<Integer, Integer, Integer, Integer, Double>> stream = env
				.fromCollection(broadData);
		// stream.keyBy(0).max(2).print();
		/*
		 * stream.keyBy(0) .reduce(new ReduceFunction<Tuple3<Integer, Integer,
		 * Integer>>() {
		 * 
		 * @Override public Tuple3<Integer, Integer, Integer> reduce(
		 * Tuple3<Integer, Integer, Integer> value1, Tuple3<Integer, Integer,
		 * Integer> value2) throws Exception { // TODO Auto-generated method
		 * stub
		 * 
		 * return new Tuple3<Integer, Integer, Integer>(value2.f0, value2.f1,
		 * value2.f2 + value1.f2); } }).print();
		 */
		// output max value
		//stream.keyBy(0).countWindow(2).max(2).print();
		// output the tuple of max value
		stream.keyBy(0).countWindow(8).process(new ProcessWindowFunction<Tuple5<Integer,Integer,Integer,Integer,Double>,Tuple5<Integer,Integer,Integer,Integer,Double>, Tuple, GlobalWindow>() {

			@Override
			public void process(
					Tuple arg0,
					ProcessWindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple, GlobalWindow>.Context arg1,
					Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> arg2,
					Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> out)
					throws Exception {
				arg2.forEach(t->{
					out.collect(t);
				});
				
			}
		}).keyBy(1).countWindow(4).process(new ProcessWindowFunction<Tuple5<Integer,Integer,Integer,Integer,Double>,Tuple5<Integer,Integer,Integer,Integer,Double>, Tuple, GlobalWindow>() {

			@Override
			public void process(
					Tuple arg0,
					ProcessWindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple, GlobalWindow>.Context arg1,
					Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> arg2,
					Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> out)
					throws Exception {
				arg2.forEach(t->{
					out.collect(t);
				});
				
			}
		}).keyBy(2).countWindow(2).minBy(4).keyBy(0).countWindow(2).process(new ProcessWindowFunction<Tuple5<Integer,Integer,Integer,Integer,Double>,Tuple5<Integer,Integer,Integer,Integer,Double>, Tuple, GlobalWindow>() {

			@Override
			public void process(
					Tuple arg0,
					ProcessWindowFunction<Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple5<Integer, Integer, Integer, Integer, Double>, Tuple, GlobalWindow>.Context arg1,
					Iterable<Tuple5<Integer, Integer, Integer, Integer, Double>> arg2,
					Collector<Tuple5<Integer, Integer, Integer, Integer, Double>> out)
					throws Exception {
				arg2.forEach(t->{
					out.collect(t);
				});
				
			}
		}).keyBy(1).countWindow(2).maxBy(4).print();
		env.execute();
	}
}
