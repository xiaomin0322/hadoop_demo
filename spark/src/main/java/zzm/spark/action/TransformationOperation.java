package zzm.spark.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.derby.tools.sysinfo;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.sysFuncNames_return;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * transformation操作实战
 * 
 * @author dd
 * 
 */
public class TransformationOperation {
	public static void main(String[] args) {
		// mapTest();
		// filterTest();
		// flatMapTest();
		// groupByKeyTest();
		// reduceByKeyTest();
		// sortByKeyTest();
		// joinTest();
		//aggregateByKeyTest();
		//unionTest();
		cacheTest();
		
		//groupByKeyCacheTest();
	}
	
	/**
	 * groupByKey算子 案例：按照班级对成绩进行分组
	 */
	private static void groupByKeyCacheTest() {
		SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));

		// 创建JavaPairRDD
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);

		JavaPairRDD<String, Iterable<Integer>> groupScores = scoresRDD
				.groupByKey();
		
		

		groupScores
				.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {

					@Override
					public void call(Tuple2<String, Iterable<Integer>> arg0)
							throws Exception {
						// TODO Auto-generated method stub
						System.out.println("class:" + arg0._1);
						Iterator<Integer> it = arg0._2.iterator();
						while (it.hasNext()) {
							System.out.println(it.next());
						}
						System.out
								.println("====================================");
					}
				});
		
		groupScores.count();

		sc.close();
	}

	
	/**
	 *cacheTest算子案例： 将集合中的元素都乘以2
	 */
	private static void cacheTest() {
		SparkConf conf = new SparkConf().setAppName("map")/*.setMaster("local")*/;
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> numbers = new ArrayList<String>();

		for(int i=0;i<10000;i++){
			numbers.add(""+i);
		}
		
		JavaRDD<String> numberRDD = sc.parallelize(numbers).cache();

		 long numAs = numberRDD.filter(new Function<String, Boolean>() {
		      public Boolean call(String s) { return s.contains("3"); }
		    }).count();

		    long numBs = numberRDD.filter(new Function<String, Boolean>() {
		      public Boolean call(String s) { return s.contains("11"); }
		    }).count();

		    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		
		
		
		
		
		//String str= numberRDD.toDebugString();
        //System.out.println(str);
		sc.stop();
	}
	
	
	/**
	 * testSparkCoreApiUnion 合并两个RDD
	 * 
	 * @param rdd
	 */
	private static void unionTest() {
		
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> studentsList = Arrays.asList(
				new Tuple2<String, Integer>("a", 1),
				new Tuple2<String, Integer>("b", 2),
				new Tuple2<String, Integer>("c", 8));
		List<Tuple2<String, Integer>> scoresList = Arrays.asList(
				new Tuple2<String, Integer>("a", 4),
				new Tuple2<String, Integer>("b", 3),
				new Tuple2<String, Integer>("c", 1));
		
		// 并行化两个集合
				JavaPairRDD<String, Integer> studentsRDD = spark
						.parallelizePairs(studentsList);
				JavaPairRDD<String, Integer> scoresRDD = spark
						.parallelizePairs(scoresList);
		
		
		JavaPairRDD<String, Integer> unionRdd = studentsRDD.union(scoresRDD);
		unionRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println("key=="+t._1+"   "+t._2);
			}
		
		});
	}

	/**
	 * aggregateByKey函数对PairRDD中相同Key的值进行聚合操作，在聚合过程中同样使用了一个中立的初始值。
	 * 和aggregate函数类似，aggregateByKey返回值的类型不需要和RDD中value的类型一致。
	 * 因为aggregateByKey是对相同Key中的值进行聚合操作，所以aggregateByKey函数最终返回的类型还是Pair RDD，
	 * 对应的结果是Key和聚合好的值；而aggregate函数直接是返回非RDD的结果，这点需要注意。
	 * 在实现过程中，定义了三个aggregateByKey函数原型，但最终调用的aggregateByKey函数都一致。
	 * 
	 * 
	 * aggregateByKey和aggregate结果有点不一样。
	 * 如果用aggregate函数对含有3、2、4三个元素的RDD进行计算，初始值为1的时候，计算的结果应该是10，而这里是9，
	 * 这是因为aggregate函数中的初始值需要和reduce函数以及combine函数结合计算
	 * ，而aggregateByKey中的初始值只需要和reduce函数计算， 不需要和combine函数结合计算，所以导致结果有点不一样。
	 */
	public static void aggregateByKeyTest() {
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext spark = new JavaSparkContext(conf);

		Tuple2<Integer, Integer> t1 = new Tuple2<Integer, Integer>(1, 3);
		Tuple2<Integer, Integer> t2 = new Tuple2<Integer, Integer>(1, 2);
		Tuple2<Integer, Integer> t3 = new Tuple2<Integer, Integer>(1, 4);
		Tuple2<Integer, Integer> t4 = new Tuple2<Integer, Integer>(2, 3);
		List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();
		System.out.println("原始数据如下:");
		list.add(t1);
		list.add(t2);
		list.add(t3);
		list.add(t4);

		list.forEach(t -> {
			System.out.println(t._1 + "," + t._2());
		});
		System.out.println("====================================");

		JavaPairRDD<Integer, Integer> rdd1 = spark.parallelizePairs(list);
		JavaPairRDD<Integer, Integer> result = rdd1.aggregateByKey(11,
				(a, b) -> {
					System.out.println("seqOp:" + a + "," + b);
					return Math.max(a, b);
				}, (a, b) -> {
					System.out.println("combOp:" + a + "," + b);
					return a + b;
				});
		rdd1.foreach(System.out::println);
		System.out.println("聚合返回结果:" + result.collectAsMap());
	}

	/**
	 * map算子案例： 将集合中的元素都乘以2
	 */
	private static void mapTest() {
		SparkConf conf = new SparkConf().setAppName("map").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);

		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

		JavaRDD<Integer> multipleNumberRDD = numberRDD
				.map(new Function<Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0 * 2;
					}
				});

		multipleNumberRDD.foreach(new VoidFunction<Integer>() {

			@Override
			public void call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.print(arg0 + " ");
			}
		});

		sc.close();
	}

	/**
	 * filter算子案例： 过滤集合中的偶数
	 */
	private static void filterTest() {
		SparkConf conf = new SparkConf().setAppName("filter")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

		// filter算子传入的也是Function，call方法的返回值是Boolean
		// 每一个初始RDD中的元素都会传入call方法，如果想在新的RDD中保留该元素则返回true，否则返回false
		JavaRDD<Integer> evenNumberRDD = numberRDD
				.filter(new Function<Integer, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Integer arg0) throws Exception {
						// TODO Auto-generated method stub
						return arg0 % 2 == 0;
					}
				});

		evenNumberRDD.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer arg0) throws Exception {
				System.out.println(arg0);

			}
		});

		sc.close();
	}

	/**
	 * flatMap算zi 拆分一行文本的单词
	 */
	private static void flatMapTest() {
		SparkConf conf = new SparkConf().setAppName("faltMap").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> lineList = Arrays.asList("hello you", "hello me",
				"hello world");

		JavaRDD<String> lines = sc.parallelize(lineList);

		/*
		 * 对RDD执行flatMap算子将每一行文本拆分为多个单词
		 * flatMap其实就是接收原始RDD中的每个元素，并进行各种处理返回多个元素，即封装在Iterable中
		 * 新的RDD中，即封装了所有的新元素，所以新的RDD大小一定大于原始的RDD
		 */
		JavaRDD<String> words = lines
				.flatMap(new FlatMapFunction<String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<String> call(String arg0) throws Exception {
						// TODO Auto-generated method stub
						return Arrays.asList(arg0.split(" ")).iterator();
					}
				});

		words.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
		});

		sc.close();
	}

	/**
	 * groupByKey算子 案例：按照班级对成绩进行分组
	 */
	private static void groupByKeyTest() {
		SparkConf conf = new SparkConf().setAppName("groupByKey").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));

		// 创建JavaPairRDD
		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);

		JavaPairRDD<String, Iterable<Integer>> groupScores = scoresRDD
				.groupByKey();

		groupScores
				.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {

					@Override
					public void call(Tuple2<String, Iterable<Integer>> arg0)
							throws Exception {
						// TODO Auto-generated method stub
						System.out.println("class:" + arg0._1);
						Iterator<Integer> it = arg0._2.iterator();
						while (it.hasNext()) {
							System.out.println(it.next());
						}
						System.out
								.println("====================================");
					}
				});

		sc.close();
	}

	/**
	 * reduceByKey算子 案例：求各个班级总分
	 */
	private static void reduceByKeyTest() {
		SparkConf conf = new SparkConf().setAppName("reduceByKey").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String, Integer>> scores = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));

		JavaPairRDD<String, Integer> scoresRDD = sc.parallelizePairs(scores);

		JavaPairRDD<String, Integer> totalScores = scoresRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer arg0, Integer arg1)
							throws Exception {
						// TODO Auto-generated method stub
						return arg0 + arg1;
					}
				});

		totalScores.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			@Override
			public void call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0._1 + " : " + arg0._2);
			}
		});

		sc.close();
	}

	/**
	 * sortByKey算子 案例：对学生成绩进行排序
	 */
	private static void sortByKeyTest() {
		SparkConf conf = new SparkConf().setAppName("sortByKey").setMaster(
				"local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> scores = Arrays.asList(
				new Tuple2<Integer, String>(10, "leo"),
				new Tuple2<Integer, String>(100, "ksc"),
				new Tuple2<Integer, String>(99, "my"),
				new Tuple2<Integer, String>(80, "jack"));

		JavaPairRDD<Integer, String> scoresRDD = sc.parallelizePairs(scores);

		// 默认true升序，false降序
		JavaPairRDD<Integer, String> sortedRDD = scoresRDD.sortByKey();

		sortedRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {

			@Override
			public void call(Tuple2<Integer, String> arg0) throws Exception {
				System.out.println(arg0._1 + ": " + arg0._2);

			}
		});

		sc.close();
	}

	/**
	 * join 案例：打印学生成绩
	 */
	private static void joinTest() {
		SparkConf conf = new SparkConf().setAppName("joinandCogroup")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> studentsList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(3, "tom"));
		List<Tuple2<Integer, Integer>> scoresList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(3, 60));

		// 并行化两个集合
		JavaPairRDD<Integer, String> studentsRDD = sc
				.parallelizePairs(studentsList);
		JavaPairRDD<Integer, Integer> scoresRDD = sc
				.parallelizePairs(scoresList);

		// 使用join算子关联两个RDD
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentscores = studentsRDD
				.join(scoresRDD);

		studentscores
				.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {

					@Override
					public void call(
							Tuple2<Integer, Tuple2<String, Integer>> arg0)
							throws Exception {
						// TODO Auto-generated method stub
						System.out.println("student id : " + arg0._1);
						System.out.println("student name： " + arg0._2._1);
						System.out.println("student score: " + arg0._2._2);
						System.out
								.println("==========================================");
					}
				});
	}
}