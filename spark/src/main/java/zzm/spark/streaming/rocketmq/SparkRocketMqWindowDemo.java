package zzm.spark.streaming.rocketmq;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import scala.Tuple2;

import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.cmall.mq.rocket.producer.DefaultProducer;

/**
 * 基于滑动窗口的热点搜索词实时统计
 * nc -lp 9999
 * @author Administrator 每隔5秒钟，统计最近20秒钟的搜索词的搜索频次，并打印出排名最靠前的3个搜索词以及出现次数
 *
 */
public class SparkRocketMqWindowDemo {
	
	static  ApplicationContext    context= new ClassPathXmlApplicationContext(new String[]{"classpath:spark-rocketmq-consumer.xml" });
	
	
	public static void main(String[] args) throws Exception {
		
		
		SparkConf conf = new SparkConf().setAppName("SparkSqlRocketMqWindowDemo")/*.setMaster("local[2]")*/;
		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(5));

		// 从nc服务中读取输入的数据
		JavaReceiverInputDStream<String> socketTextStream = jsc
				.receiverStream(new SparkRocketMqReceiver());

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
						}, Durations.seconds(60 * 60 * 24), Durations.seconds(5));

		/**
		 * 获取前3名的搜索词
		 */
		JavaPairDStream<String, Integer> resultDStream = reduceByKeyAndWindowDStream
				.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {

					private static final long serialVersionUID = 1L;

					public JavaPairRDD<String, Integer> call(
							JavaPairRDD<String, Integer> wordsRDD)
							throws Exception {

						// 通过mapToPair算子，将key与value互换位置
						JavaPairRDD<Integer, String> mapToPairRDD = wordsRDD
								.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

									private static final long serialVersionUID = 1L;

									public Tuple2<Integer, String> call(
											Tuple2<String, Integer> tuple)
											throws Exception {
										// 将key与value互换位置
										return new Tuple2<Integer, String>(
												tuple._2, tuple._1);
									}
								});

						// 根据key值进行降序排列
						JavaPairRDD<Integer, String> sortByKeyRDD = mapToPairRDD
								.sortByKey(false);

						// 然后再次执行反转，变成(searchWord, count)的这种格式
						JavaPairRDD<String, Integer> wordcountRDD = sortByKeyRDD
								.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

									private static final long serialVersionUID = 1L;

									public Tuple2<String, Integer> call(
											Tuple2<Integer, String> tuple)
											throws Exception {

										return new Tuple2<String, Integer>(
												tuple._2, tuple._1);
									}
								});
						

						// 获取降序排列之后的前3名
						List<Tuple2<String, Integer>> result = wordcountRDD
								.take(3);

                        for(Tuple2<String, Integer> rs:result){
                        	if(rs == null){
                        		continue;
                        	}
                        	 try {
    							 Message msg = new Message("TopicTest2",// topic
    					                    "TagA",// tag
    					                    "key113",// key
    					                    (rs._1 + " "+ rs._2).getBytes());// body
    							 DefaultProducer mqProducer= context.getBean(DefaultProducer.class);
    							 SendResult sendResult = mqProducer.getDefaultMQProducer().send(msg);
    							 System.out.println(sendResult);       
    						} catch (Exception e) {
    							e.printStackTrace();
    						}
                        }

						return wordcountRDD;
					}
				});

		jsc.start();
		jsc.awaitTermination();
		jsc.close();
	}
}