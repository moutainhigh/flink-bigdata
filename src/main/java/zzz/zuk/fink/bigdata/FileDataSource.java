package zzz.zuk.fink.bigdata;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FileDataSource {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		//文件中获取
		String filePath = "text";
		DataSource<String> datasource = env.readTextFile(filePath);
		datasource.print();
		
		FlatMapOperator<String, Tuple2<String, Integer>> flatMapOperator =
		datasource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

			@Override
			public void flatMap(String arg0, Collector<Tuple2<String, Integer>> arg1) throws Exception {
				arg1.collect(new Tuple2<String, Integer>(arg0, 1));
			}
		});
		
		flatMapOperator.print();
		
		AggregateOperator<Tuple2<String, Integer>> ao =flatMapOperator.groupBy(0).sum(1);
		
		ao.print();
		
		//输出到文件中
		ao.writeAsText("/Users/huangliao/Desktop/textOutput1.txt")
		  .setParallelism(1);
		ao.writeAsCsv("/Users/huangliao/Desktop/textOutput2.txt")
		  .setParallelism(1);
		
		env.execute();
		
	}
	
}
