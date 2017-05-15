package zzm.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
@SuppressWarnings("deprecation")
public class SparkSqlHiveTest {
	
	 public static final String master = "spark://master:7077";
	   
		public static void main(String[] args) {
	        SparkConf conf = new SparkConf().setAppName("demo1").setMaster(master);
	        conf.set("spark.executor.memory", "256M");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        HiveContext sqlContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());
	        //Dataset<Row> df = sqlContext.sql("select * from data_center.shop limit 10");
	       /* DataFrame df = sqlContext.sql("select * from data_center.shop limit 10");  
	        
	        Row[] rows = (Row[]) df.collect();
	        for(Row row : rows){
	            System.out.println(row);
	        }*/
	        sqlContext.sql("select * from data_center.shop limit 10").show();
	        
	    }
}
