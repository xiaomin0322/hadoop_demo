package zzm.spark.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * /spark/bin/spark-submit --master local[2] --jars
 * /spark/examples/jars/mysql-connector-java-5.1.42-bin.jar --class
 * zzm.spark.streaming.UpdateStateByKeyDemo --executor-memory 512m
 * --total-executor-cores 2 --driver-memory 512M
 * /spark/examples/jars/UpdateStateByKeyDemo.jar
 * 
 * @author ThinkPad
 *
 */
public class UpdateStateByKeyCheckPointDemo {

	static String checkpointDirectory = "/usr";

	public static JavaStreamingContext getJavaStreamingContext() {
		SparkConf conf = new SparkConf()
				/* .setMaster("local[2]") */.setAppName("UpdateStateByKeyCheckPointDemo");
		JavaStreamingContext jsc = new JavaStreamingContext(conf,
				Durations.seconds(1));

		// jsc.checkpoint("/usr/local/tmp/checkpoint");
		// 保存在hadoop usr 目录，spark master配置好
		// 本地跑连接不上hadoop node会有问题
		jsc.checkpoint(checkpointDirectory);

		JavaReceiverInputDStream lines = jsc.socketTextStream("192.168.1.246",
				9999);

		JavaDStream<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() { // 如果是Scala，由于SAM转换，所以可以写成val
																	// words =
																	// lines.flatMap
																	// { line =>
																	// line.split(" ")}

					public Iterator<String> call(String line) throws Exception {
						return Arrays.asList(line.split(" ")).iterator();

					}

				});

		JavaPairDStream<String, Integer> pairs = words
				.mapToPair(new PairFunction<String, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(String word)
							throws Exception {

						return new Tuple2<String, Integer>(word, 1);

					}

				});

		JavaPairDStream<String, Integer> wordsCount = pairs
				.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
					// 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
					private static final long serialVersionUID = 1L;

					public Optional<Integer> call(List<Integer> values,
							Optional<Integer> state)

					throws Exception {

						Integer updatedValue = 0;

						if (state.isPresent()) {

							updatedValue = state.get();

						}

						for (Integer value : values) {

							updatedValue += value;

						}

						return Optional.of(updatedValue);

					}

				});

		wordsCount.print();
		return jsc;
	}

	public static void main(String[] args) throws Exception {

		org.apache.spark.api.java.function.Function0<JavaStreamingContext> createContextFunc = () -> getJavaStreamingContext();

		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(
				checkpointDirectory, createContextFunc);

		jsc.start();

		jsc.awaitTermination();

		jsc.close();
	}
}