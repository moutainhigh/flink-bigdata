package zzz.zuk.fink.bigdata;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

/***
 * 解决:
 * 1.数据倾斜，
 * 2.数据平衡，
 * 3.数据分区
 * @author huangliao
 *
 */
public class Partition {

	public static void main(String[] args)  throws Exception {
		
		List<Tuple2<Integer, String>> data = Lists.newArrayList();
		data.add(new Tuple2<>(1,"1"));
		data.add(new Tuple2<>(2,"2"));
		data.add(new Tuple2<>(3,"3"));
		data.add(new Tuple2<>(4,"4"));
		data.add(new Tuple2<>(5,"5"));
		data.add(new Tuple2<>(6,"6"));
		data.add(new Tuple2<>(7,"7"));
		
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		DataSource<Tuple2<Integer, String>> datasource = env.fromCollection(data);
		
		rebalance(datasource);
		partitionHashMap(datasource);
		partitionRange(datasource);
		partitionCostom(datasource);
		
	}
	
	/***
	 * 数据再平衡
	 * @param datasource
	 * @throws Exception 
	 */
	public static void rebalance(DataSource<Tuple2<Integer, String>> datasource) throws Exception{
		
		datasource.rebalance().map(new MapFunction<Tuple2<Integer,String>, String>() {

			@Override
			public String map(Tuple2<Integer, String> arg0) throws Exception {
				String str = "thread-id:"+Thread.currentThread().getId() + "-" + arg0.f1;
				System.out.println(str);
				return str;
			}
		}).print();
		
	}
	
	/***
	 * hash分区
	 */
	@SuppressWarnings("serial")
	public static void partitionHashMap(DataSource<Tuple2<Integer, String>> datasource) throws Exception{
		
		datasource.partitionByHash(0).map(new MapFunction<Tuple2<Integer,String>, String>() {

			@Override
			public String map(Tuple2<Integer, String> arg0) throws Exception {
				String str = "thread-id:"+Thread.currentThread().getId() + "-" + arg0.f1;
				System.out.println(str);
				return str;
			}
		}).print();
		
	}
	
	/***
	 * 范围分区
	 */
	@SuppressWarnings("serial")
	public static void partitionRange(DataSource<Tuple2<Integer, String>> datasource) throws Exception{
		datasource.partitionByRange(0).map(new MapFunction<Tuple2<Integer,String>, String>() {

			@Override
			public String map(Tuple2<Integer, String> arg0) throws Exception {
				String str = "thread-id:"+Thread.currentThread().getId() + "-" + arg0.f1;
				System.out.println(str);
				return str;
			}
		}).print();
	}
	
	/***
	 * 自定义分区
	 */
	@SuppressWarnings("serial")
	public static void partitionCostom(DataSource<Tuple2<Integer, String>> datasource) throws Exception{
		
		datasource.partitionCustom(new MyPartition(), 0).map(new MapFunction<Tuple2<Integer,String>, String>() {

			@Override
			public String map(Tuple2<Integer, String> arg0) throws Exception {
				String str = "thread-id:"+Thread.currentThread().getId() + "-" + arg0.f1;
				System.out.println(str);
				return str;
			}
		}).print();
		
	}
	
	/***
	 * 自定义分区
	 * @author huangliao
	 *
	 */
	@SuppressWarnings("serial")
	public static class MyPartition implements Partitioner<Integer>{

		/***
		 * 
		 */
		@Override
		public int partition(Integer key, int numPartitions) {
			System.err.println("分区总数：" + numPartitions);
			return key % 3;
		}
		
	}
	
}
