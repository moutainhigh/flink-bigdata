package zzz.zuk.fink.bigdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.JoinOperator.EquiJoin;
import org.apache.flink.api.java.tuple.Tuple2;

/***
 * 内外连接，分组，排序等
 * @author huangliao
 *
 */
public class TransformationTable {
	
	public static void main(String[] args) throws Exception {
		
		List<Tuple2<Integer, String>> list1 = new ArrayList<>();
		list1.add(new Tuple2<Integer, String>(1,"owen"));
		list1.add(new Tuple2<Integer, String>(2,"rose"));
		list1.add(new Tuple2<Integer, String>(3,"tony"));
		list1.add(new Tuple2<Integer, String>(4,"mary"));
		list1.add(new Tuple2<Integer, String>(5,"superMan"));
		
		List<Tuple2<Integer, String>> list2 = new ArrayList<>();
		list2.add(new Tuple2<Integer, String>(3,"钢铁侠"));
		list2.add(new Tuple2<Integer, String>(4,"超级玛丽"));
		list2.add(new Tuple2<Integer, String>(5,"超人"));
		list2.add(new Tuple2<Integer, String>(6,"蜘蛛侠"));
		
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		DataSource<Tuple2<Integer, String>> datasource1 = env.fromCollection(list1);
		DataSource<Tuple2<Integer, String>> datasource2 = env.fromCollection(list2);
		
//		innerJoin(datasource1, datasource2);
//		leftJoin(datasource1, datasource2);
//		cross(datasource1, datasource2);
		
		sort(datasource1);
	}
	
	/***
	 * 排序
	 * @param datasource1
	 * @throws Exception 
	 */
	public static void sort(DataSource<Tuple2<Integer, String>> datasource1) throws Exception{
		//获取前2条
		datasource1.first(2).print();
	
		//获取前10条，并根据第一个字段降序排列
		datasource1.first(10).sortPartition(0, Order.DESCENDING).print();
		
		//根据第一个字段分组，并且获取每组的前两行
		datasource1.groupBy(0).first(2).print();
	
		//分组排序
		//根据第一个字段分组，再第二列做降序排列，再输出分组的前2行
		datasource1.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
	}
	
	/***
	 * 笛卡尔积
	 * @param datasource1
	 * @param datasource2
	 * @throws Exception 
	 */
	public static void cross(DataSource<Tuple2<Integer, String>> datasource1, DataSource<Tuple2<Integer, String>> datasource2) throws Exception{
		 datasource1.cross(datasource2).print();
	}
	
	/***
	 * 内连接
	 * @param datasource1
	 * @param datasource2
	 * @throws Exception
	 */
	@SuppressWarnings("serial")
	public static void innerJoin(DataSource<Tuple2<Integer, String>> datasource1, DataSource<Tuple2<Integer, String>> datasource2 ) throws Exception{
		
		EquiJoin<Tuple2<Integer, String>, Tuple2<Integer, String>, Person> joins = 
			datasource1.join(datasource2)
			           .where(0)
			           .equalTo(0)
			           .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Person>() {
	
						@Override
						public Person join(Tuple2<Integer, String> arg0, Tuple2<Integer, String> arg1) throws Exception {
							return new Person(arg0.f0,arg0.f1,arg1.f1);
						}
					});
		
		joins.print();
		
	}
	
	/***
	 * 左连接
	 * @param datasource1
	 * @param datasource2
	 * @throws Exception 
	 */
	@SuppressWarnings("serial")
	public static void  leftJoin(DataSource<Tuple2<Integer, String>> datasource1, DataSource<Tuple2<Integer, String>> datasource2 ) throws Exception{
		
		JoinOperator<Tuple2<Integer,String>, Tuple2<Integer,String>, Person> joins =
			datasource1.leftOuterJoin(datasource2)
			           .where(0)
			           .equalTo(0)
			           .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Person>() {
	
						@Override
						public Person join(Tuple2<Integer, String> arg0, Tuple2<Integer, String> arg1) throws Exception {
							if(arg1!=null){
								return new Person(arg0.f0,arg0.f1,arg1.f1);
							}
							else{
								return new Person(arg0.f0,arg0.f1,"");
							}
						}
					});
		
		joins.print();
//		joins.sortPartition("id", Order.ASCENDING).print();
		
	}
	
	public static class Person implements Serializable{
		private Integer id;
		private String english;
		private String chinese;
		public Person(){}
		public Person(Integer id, String english,String chinese){
			this.id = id;
			this.english = english;
			this.chinese = chinese;
		}
		@Override
		public String toString() {
			return "id:"+id+",english:"+english+",chinese:"+chinese;
		}
	}

}
