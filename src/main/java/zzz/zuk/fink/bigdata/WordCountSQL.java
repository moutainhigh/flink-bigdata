package zzz.zuk.fink.bigdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;


/***
 * 注意WC类必须实现无参构造函数
 * @author huangliao
 *
 */
public class WordCountSQL {

	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		List<String> data = new ArrayList<String>();
		data.add("to be or not to be is question");
		data.add("what is your question");
		DataSource<String> datasource = env.fromCollection(data);
		
		FlatMapOperator<String, WC> flatmapoperator =
				datasource.flatMap(new FlatMapFunction<String, WC>(){

					@Override
					public void flatMap(String arg0, Collector<WC> arg1) throws Exception {
						if(!StringUtils.isBlank(arg0)){
							String[] array = arg0.split(" ");
							if(array!=null && array.length>0){
								for (String str : array) {
									arg1.collect(new WC(str,1));
								}
							}
						}
					}
				});
		
		flatmapoperator.print();
		
		//TODO 转DataSource
		List<WC> list = flatmapoperator.collect();
		DataSet<WC> input = env.fromCollection(list);
		
		input.print();
		 
		//TODO 转SQL查询
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        tEnv.registerDataSet("WordCount", input, "word, frequency");
        Table table = tEnv.sqlQuery("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");
        DataSet<WC> result = tEnv.toDataSet(table, WC.class);
        result.print();
		
	}
	
	public static class WC{
		private String word;
		private int frequency;
		
		/***
		 * 这个构造方法很重要，不实现会报错
		 */
		public WC(){
			
		}

		public WC(String word, int frequency){
			this.word = word;
			this.frequency = frequency;
		}
		
		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public int getFrequency() {
			return frequency;
		}

		public void setFrequency(int frequency) {
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "word:"+word+",frequency:"+frequency;
		}
		
		
	}
	
}
