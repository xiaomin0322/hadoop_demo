package zzm.spark.action;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * action操作实战
 * 
 * @author dd
 * 
 */
public class ActionOperation {
	public static void main(String[] args) {
		// reduceTest();
		 //collectTest();
		//countTest();
		// takeTest();
		countByKeyTest();
	}

	/**
	 * reduce算子 案例：求累加和
	 */
	private static void reduceTest() {
		SparkConf conf = new SparkConf().setAppName("reduce")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);

		// 使用reduce操作对集合中的数字进行累加
		int sum = numbersRDD.reduce(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0 + arg1;
			}
		});

		System.out.println(sum);

		sc.close();
	}

	/**
	 * collect算子 可以将集群上的数据拉取到本地进行遍历（不推荐使用）
	 */
	private static void collectTest() {
		SparkConf conf = new SparkConf().setAppName("collect").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);

		JavaRDD<Integer> doubleNumbers = numbersRDD
				.map(new Function<Integer, Integer>() {

					@Override
					public Integer call(Integer arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0 * 2;
					}
				});

		// foreach的action操作是在远程集群上遍历rdd中的元素，而collect操作是将在分布式集群上的rdd
		// 数据拉取到本地，这种方式一般不建议使用，因为如果rdd中的数据量较大的话，比如超过一万条，那么性能会
		// 比较差，因为要从远程走大量的网络传输，将数据获取到本地，有时还可能发生oom异常，内存溢出
		// 所以还是推荐使用foreach操作来对最终的rdd进行处理
		List<Integer> doubleNumList = doubleNumbers.collect();
		for (Integer num : doubleNumList) {
			System.out.println(num);
		}
		sc.close();
	}

	/**
	 * count算子 可以统计rdd中的元素个数
	 */
	private static void countTest() {
		SparkConf conf = new SparkConf().setAppName("count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);

		// 对rdd使用count操作统计rdd中元素的个数
		long count = numbersRDD.count();
		System.out.println(count);

		sc.close();
	}

	/**
	 * take算子 将远程rdd的前n个数据拉取到本地
	 */
	private static void takeTest() {
		SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);

		// take操作与collect操作类似，也是从远程集群上获取rdd数据，但是，collect操作获取的是rdd的
		// 所有数据，take获取的只是前n个数据
		List<Integer> top3number = numbersRDD.take(3);
		for (Integer num : top3number) {
			System.out.println(num);
		}
		sc.close();
	}

	/**
	 * saveAsTextFile算子
	 * 
	 */
	private static void saveAsTExtFileTest() {
		SparkConf conf = new SparkConf().setAppName("saveAsTextFile");

		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);

		JavaRDD<Integer> doubleNumbers = numbersRDD
				.map(new Function<Integer, Integer>() {

					@Override
					public Integer call(Integer arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0 * 2;
					}
				});

		// saveAsTextFile算子可以直接将rdd中的数据保存在hdfs中
		// 但是我们在这里只能指定保存的文件夹也就是目录，那么实际上，会保存为目录中的
		// /double_number.txt/part-00000文件
		doubleNumbers.saveAsTextFile("hdfs://spark1:9000/double_number.txt");

		sc.close();
	}

	/**
	 * countByKey算子
	 */

	private static void countByKeyTest() {
		SparkConf conf = new SparkConf().setAppName("take").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String, String>> studentsList = Arrays.asList(
				new Tuple2<String, String>("class1", "leo"),
				new Tuple2<String, String>("class2", "jack"),
				new Tuple2<String, String>("class1", "marry"),
				new Tuple2<String, String>("class2", "tom"),
				new Tuple2<String, String>("class2", "david"));

		JavaPairRDD<String, String> studentsRDD = sc
				.parallelizePairs(studentsList);

		// countByKey算子可以统计每个key对应元素的个数
		// countByKey返回的类型直接就是Map<String,Object>

		Map<String, Long> studentsCounts = studentsRDD.countByKey();

		for (Map.Entry<String, Long> studentsCount : studentsCounts
				.entrySet()) {
			System.out.println(studentsCount.getKey() + " : "
					+ studentsCount.getValue());
		}
		sc.close();
	}
}