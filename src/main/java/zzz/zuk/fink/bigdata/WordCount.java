package zzz.zuk.fink.bigdata;

import java.security.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/***
 * 单词计数
 * @author huangliao
 *
 */

public class WordCount {
	
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		
		//报错，重试3次，5秒
		env.setNumberOfExecutionRetries(3);
		RestartStrategyConfiguration restartStrategyConfiguration = RestartStrategies.fixedDelayRestart(3, Time.seconds(5));
		env.setRestartStrategy(restartStrategyConfiguration);
		
		List<String> data = new ArrayList<String>();
		data.add("to be or not to be is question");
		data.add("what is your question");
		DataSource<String> datasource = env.fromCollection(data);
		
		datasource.print();
		
		FlatMapOperator<String, Tuple2<String,Integer>> flatmapoperator =
		datasource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){

			@Override
			public void flatMap(String arg0, Collector<Tuple2<String, Integer>> arg1) throws Exception {
				if(!StringUtils.isBlank(arg0)){
					String[] array = arg0.split(" ");
					if(array!=null && array.length>0){
						for (String str : array) {
							Tuple2<String, Integer> tuple = new Tuple2<>(str,1);
							arg1.collect(tuple);
						}
					}
				}
			}
		});
		
		AggregateOperator<Tuple2<String, Integer>> tupleResult = flatmapoperator.groupBy(0).sum(1).setParallelism(1);
		
		tupleResult.print();
		
	}
	
	

}
