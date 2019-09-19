package zzz.zuk.fink.bigdata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Transformation {

	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		List<String> data = new ArrayList<>();
		data.add("hello");
		data.add("world");
		data.add("hello");
		data.add("szu");
		data.add("szu");

//		data.add("1szu");
//		data.add("2szu");
//		data.add("3szu");
//		data.add("4szu");
//		data.add("5szu");
//		data.add("6szu");
//		data.add("7szu");
//		data.add("8szu");
//		data.add("9szu");
//		data.add("10szu");
//		data.add("11szu");
//		data.add("12szu");
//		data.add("13szu");
//		data.add("14szu");
//		data.add("15szu");
//		data.add("16szu");
//		data.add("17szu");
//		data.add("18szu");
		DataSource<String> datasource = env.fromCollection(data);
		
//		map(datasource);
//		flatmap(datasource);
//		filter(datasource);
//		mapPartition(datasource);
		reduce(datasource);
	}
	
	/**
	 * @throws Exception *
	 * 对数据进行聚合操作，结合当前元素与上一次reduce返回的值进行混合操作
	 */
	@SuppressWarnings("serial")
	public static void reduce(DataSource<String> datasource) throws Exception{
		
		org.apache.flink.api.java.operators.ReduceOperator<String> reduceOpt =
		datasource.reduce(new ReduceFunction<String>() {
			
			/***
			 * 一，第一次执行时，arg0与arg1分别为集合第一个和第二个数值；
			 * 二，往后的执行，arg0为第一次返回的结果，arg1为新的数值
			 */
			@Override
			public String reduce(String arg0, String arg1) throws Exception {
				System.out.println(arg0+":"+arg1);
				return "[" + arg0 + "->" +arg1 + "]";
			}
		});
		
		reduceOpt.print();
		
	}
	
	/***
	 * 每次处理一个分区的数据
	 * @param datasource
	 * @throws Exception 
	 */
	@SuppressWarnings("serial")
	public static void mapPartition(DataSource<String> datasource) throws Exception{
		
		
		
		MapPartitionOperator<String, Tuple2<String, Integer>> mapPartitionOpt =
		datasource.mapPartition(new MapPartitionFunction<String, Tuple2<String, Integer>>() {

			/***
			 * 每次处理一个分区的数据
			 */
			@Override
			public void mapPartition(Iterable<String> arg0, Collector<Tuple2<String, Integer>> arg1) throws Exception {
				
				if(arg0!=null){
					Iterator<String> iterator = arg0.iterator();
					int i=0;
					while(iterator.hasNext()){
						String str = iterator.next();
						arg1.collect(new Tuple2<String, Integer>(i+++":"+str+"-"+Thread.currentThread().getId(), 1));
					}
				}
				
			}
		});
		
		mapPartitionOpt.print();
		
	}
	
	/***
	 * 对输入到元素执行boolean操作，保存返回为true到数值
	 * @param datasource
	 * @throws Exception 
	 */
	@SuppressWarnings("serial")
	public static void filter(DataSource<String> datasource) throws Exception{
		FilterOperator<String> filterOpt =
		datasource.filter(new FilterFunction<String>() {
			
			@Override
			public boolean filter(String arg0) throws Exception {
				if("hello".equals(arg0)){
					return false;
				}
				return true;
			}
		});
		
		filterOpt.print();
	}
	
	/***
	 * 一对多，多对多
	 * @param datasource
	 * @throws Exception 
	 */
	@SuppressWarnings("serial")
	public static void flatmap(DataSource<String> datasource) throws Exception{
		
		FlatMapOperator<String, Tuple2<String, Integer>> flatmapOpt =
		datasource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

			/***
			 * 一对多
			 * 将一个字符串转换成为一个集合
			 */
			@Override
			public void flatMap(String arg0, Collector<Tuple2<String, Integer>> arg1) throws Exception {
				if(!StringUtils.isBlank(arg0)){
					for (int i=0; i<arg0.length(); i++) {
						arg1.collect(new Tuple2<String, Integer>(arg0.charAt(i)+"", 1));
					}
				}
			}
		});
		
		flatmapOpt.print();
		System.out.println("==================");
		flatmapOpt.groupBy(0).sum(1).print();
		
	}
	
	/***
	 * 一对一转换
	 * @param datasource
	 * @throws Exception
	 */
	@SuppressWarnings("serial")
	public static void map(DataSource<String> datasource) throws Exception{
		
		MapOperator<String, Tuple2<String, Integer>> mapOperate = datasource.map(new MapFunction<String, Tuple2<String, Integer>>() {

			/***
			 * 读入一个元素，返回一个元素
			 */
			@Override
			public Tuple2<String, Integer> map(String arg0) throws Exception {
				System.out.println("arg0:"+arg0);
				Tuple2<String, Integer> tuple = new Tuple2<String,Integer>(arg0,1);
				return tuple;
			}
			
		});
		
		mapOperate.print();
		
	}
	
}
