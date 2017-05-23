package zzm.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/**
 * 基于滑动窗口的热点搜索词实时统计
 * nc -lp 9999
 * @author Administrator 每隔5秒钟，统计最近20秒钟的搜索词的搜索频次，并打印出排名最靠前的3个搜索词以及出现次数
 *
 */
public class SparkSqlWindowDemo {
	public static void main(String[] args) throws Exception {
		SparkConf conf = new SparkConf().setAppName("WindowDemo").setMaster(
				"local[2]");
		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(5));

		// 从nc服务中读取输入的数据
		JavaReceiverInputDStream<String> socketTextStream = jsc
				.socketTextStream("192.168.1.246", 9999);

		/**
		 * 搜索的日志格式：name words，比如：张三 hello 我们通过map算子将搜索词取出
		 */
		JavaDStream<String> mapDStream = socketTextStream
				.flatMap(new FlatMapFunction<String, String>() {
					public Iterator<String> call(String x) {
						return Arrays.asList(x.split(" ")).iterator();
					}
				});

		// 将搜索词映射为(searchWord, 1)的tuple格式
		JavaPairDStream<String, Integer> mapToPairDStream = mapDStream
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String searchWord)
							throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(searchWord, 1);
					}
				});

		/**
		 * 对滑动窗口进行reduceByKeyAndWindow操作 其中，窗口长度是20秒，滑动时间间隔是5秒
		 */
		JavaPairDStream<String, Integer> reduceByKeyAndWindowDStream = mapToPairDStream
				.reduceByKeyAndWindow(
						new Function2<Integer, Integer, Integer>() {

							public Integer call(Integer v1, Integer v2)
									throws Exception {
								// TODO Auto-generated method stub
								return v1 + v2;
							}
						}, Durations.seconds(20), Durations.seconds(5));

		reduceByKeyAndWindowDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				JavaRDD<Row> mapToPairRDD = rdd
						.map(new Function<Tuple2<String,Integer>, Row>() {
							@Override
							public Row call(Tuple2<String, Integer> v1)
									throws Exception {
								Row row = RowFactory.create(v1._1,v1._2);
								return row;
							}
							
						});
				
				//构建DataFrame用下面代码
				StructType structType =  DataTypes.createStructType(Arrays.asList(
						DataTypes.createStructField("item", DataTypes.StringType, true),
						DataTypes.createStructField("count", DataTypes.IntegerType, true)
		        ));
				
				SQLContext sqlContext = new SQLContext(rdd.context());
		        //SQLContext sqlContext = new org.apache.spark.sql.hive.HiveContext(rdd.context());
		        //真正创建DataFrame
		        Dataset<Row> df =  sqlContext.createDataFrame(mapToPairRDD, structType);
		      //注册一个表，用sql去操作
		        sqlContext.registerDataFrameAsTable(df,"categoryItemTable");
				
		        
		        Dataset<Row>  reseltDataFram = sqlContext.sql("SELECT * from categoryItemTable order by count desc limit 3");
		        
		        //reseltDataFram.show();
		        
		        //把DF变成RDD
		        RDD<Row> resultRowRDD = reseltDataFram.rdd();
		        
                resultRowRDD.foreachPartition(new JavaForeachPartitionFunc() {
					@Override
					public void call(scala.collection.Iterator<Row> it) {
						 while (it.hasNext()){
							 Row t =it.next();
							 System.out.println("item === "+t.getAs("item")+" "+t.getAs("count"));
					         //System.out.println("toString === "+t);
					        }
					}
				});
		        
		        
		       /* resultRowRDD.foreachPartition(new AbstractFunction1<scala.collection.Iterator<Row>,BoxedUnit>() {
					@Override
					public BoxedUnit apply(scala.collection.Iterator<Row> it) {
						 while (it.hasNext()){
					            System.out.println(it.next().toString());
					        }
					        return BoxedUnit.UNIT;
					}
				});*/
		        
			}
		});

		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}