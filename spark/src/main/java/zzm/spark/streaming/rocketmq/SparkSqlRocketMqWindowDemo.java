package zzm.spark.streaming.rocketmq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import scala.Tuple2;
import zzm.spark.streaming.JavaForeachPartitionFunc;

import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.cmall.mq.rocket.producer.DefaultProducer;

/**
 * 基于滑动窗口的热点搜索词实时统计
 * nc -lp 9999
 * @author Administrator 每隔5秒钟，统计最近20秒钟的搜索词的搜索频次，并打印出排名最靠前的3个搜索词以及出现次数
 *
 */
public class SparkSqlRocketMqWindowDemo {
	
	static  ApplicationContext    context= new ClassPathXmlApplicationContext(new String[]{"classpath:spark-rocketmq-consumer.xml" });
	
	
	public static void main(String[] args) throws Exception {
		
		
		SparkConf conf = new SparkConf().setAppName("SparkSqlRocketMqWindowDemo")/*.setMaster("local[2]")*/;
		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(5));
		
		//jsc.union(first, rest)

		// 从nc服务中读取输入的数据
		JavaReceiverInputDStream<String> socketTextStream = jsc
				.receiverStream(new SparkRocketMqReceiver2());

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
						}, Durations.seconds(60 * 60 * 10), Durations.seconds(5));

		reduceByKeyAndWindowDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			@Override
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				
				
				/*JavaRDD<Row> mapToPairRDD = rdd
						.map(new Function<Tuple2<String,Integer>, Row>() {
							@Override
							public Row call(Tuple2<String, Integer> v1)
									throws Exception {
								Row row = RowFactory.create(v1._1,v1._2);
								return row;
							}
							
						});*/
				
				JavaRDD<Row> mapToPairRDD = rdd
						.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Integer>>, Row>() {
							@Override
							public Iterator<Row> call(
									Iterator<Tuple2<String, Integer>> ts)
									throws Exception {
								List<Row> rows = new ArrayList<Row>();
								while(ts.hasNext()){
									Tuple2<String, Integer> v1 = ts.next();
									Row row = RowFactory.create(v1._1,v1._2);
									rows.add(row);
								}
								return rows.iterator();
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
		        
		        //一共获取多少条数据
			       Dataset<Row>  reseltDataFramConut =sqlContext.sql("SELECT sum(count) cnt from categoryItemTable");
			       RDD<Row> resultRowRDDCount = reseltDataFramConut.rdd();
			       Long count = null;
			       Row row = resultRowRDDCount.first();
			       if(row!=null && !row.anyNull()){
			    	    count = row.getAs("cnt");
			       }
			     Message msg = new Message("TopicTest2",// topic
		                    "TagA",// tag
		                    "key113",// key
		                    ("count================="+count).getBytes());// body
				 DefaultProducer mqProducer= context.getBean(DefaultProducer.class);
				 SendResult sendResult = mqProducer.getDefaultMQProducer().send(msg);
				 
			     System.out.println("count >>>>>>>>>>>>>>>>>>>>>>>>"+count);
		        
		        
		       Dataset<Row>  reseltDataFram = sqlContext.sql("SELECT * from categoryItemTable order by count desc limit 10");
		       
		        
		        //reseltDataFram.show();
		        
		        //把DF变成RDD
		        RDD<Row> resultRowRDD = reseltDataFram.rdd();
		        
		       // resultRowRDD.saveAsTextFile("D:\\data\\output.txt");
		        
		        
                resultRowRDD.foreachPartition(new JavaForeachPartitionFunc() {
					@Override
					public void call(scala.collection.Iterator<Row> it) {
						 java.util.List<String> list = new ArrayList<String>();
						 while (it.hasNext()){
							 Row t =it.next();
							 System.out.println("item === "+t.getAs("item")+" "+t.getAs("count"));
							 list.add(t.getAs("item")+" "+t.getAs("count"));
					         //System.out.println("toString === "+t);
					        }
						 
						 
						 
						 try {
							 //File out = new File("D:\\data\\output.txt");
							/* File out = new File("/spark/logs/output.txt");
							 FileUtils.writeLines(out, list,true);*/
							 
							 Message msg = new Message("TopicTest2",// topic
					                    "TagA",// tag
					                    "key113",// key
					                    (list.toString()+" count=").getBytes());// body
							 DefaultProducer mqProducer= context.getBean(DefaultProducer.class);
							 SendResult sendResult = mqProducer.getDefaultMQProducer().send(msg);
							 System.out.println(sendResult);       
						} catch (Exception e) {
							e.printStackTrace();
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