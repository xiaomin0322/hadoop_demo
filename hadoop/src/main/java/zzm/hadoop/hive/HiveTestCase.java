	package zzm.hadoop.hive;

import java.sql.Connection; 
import java.sql.DriverManager; 
import java.sql.ResultSet; 
import java.sql.SQLException; 
import java.sql.Statement; 

import org.apache.log4j.Logger; 

/** 
* Handle data through hive on eclipse 
* @time 2016\11\12 22:14 
*/ 
public class HiveTestCase { 
    private static String driverName = "org.apache.hive.jdbc.HiveDriver"; 
    private static String url = "jdbc:hive2://172.18.0.10:10000/default"; 
    private static String user = ""; 
    private static String password = ""; 
    private static String sql = ""; 
    private static ResultSet res; 
    private static final Logger log = Logger.getLogger(HiveTestCase.class); 

    public static void main(String[] args) { 
        try { 
            Class.forName(driverName); // 注册JDBC驱动 
//            Connection conn = DriverManager.getConnection(url, user, password); 

            //默认使用端口10000, 使用默认数据库，用户名密码默认 
            Connection conn = DriverManager.getConnection(url, "", "");
//            Connection conn = DriverManager.getConnection("jdbc:hive://HadoopSlave1:10000/default", "", "");
            //当然，若是3节点集群，则HadoopMaster或HadoopSlave1或HadoopSlave2都可以呢。前提是每个都安装了Hive，当然只安装一台就足够了。

            //    Statement用来执行SQL语句
            Statement stmt = conn.createStatement(); 

            // 创建的表名 
            String tableName = "aa"; 

            /** 第一步:存在就先删除 **/ 
            sql = "drop table " + tableName; 
            stmt.execute(sql); 

            /** 第二步:不存在就创建 **/ 
            sql = "create table "+tableName+"(id int,name string) row format delimited fields terminated by ','";

            // sql = "create table " + tableName + " (key int, value string) row format delimited fields terminated by '\t'";
            stmt.execute(sql); 

            // 执行“show tables”操作 
            sql = "show tables '" + tableName + "'"; 
            System.out.println("Running:" + sql); 
            res = stmt.executeQuery(sql); 
            System.out.println("执行“show tables”运行结果:"); 
            if (res.next()) { 
                System.out.println(res.getString(1)); 
            } 

            // 执行“describe table”操作 
            sql = "describe " + tableName; 
            System.out.println("Running:" + sql); 
            res = stmt.executeQuery(sql); 
            System.out.println("执行“describe table”运行结果:"); 
            while (res.next()) { 
                System.out.println(res.getString(1) + "\t" + res.getString(2)); 
            } 

            // 执行“load data into table”操作 
            String filepath = "/home/share/a.txt"; //因为是load data local inpath，所以是本地路径
            sql = "load data local inpath '" + filepath + "' into table " + tableName; 
            
//            String filepath = "/hive/data/test2_hive.txt"; //因为是load data  inpath，所以是集群路径，即hdfs://djt002/9000/hive/data/下
//            sql = "load data inpath '" + filepath + "' into table " + tableName; 
            
            System.out.println("Running:" + sql); 
            //res = stmt.executeQuery(sql);
            stmt.execute(sql);

            // 执行“select * query”操作 
            sql = "select * from " + tableName; 
            System.out.println("Running:" + sql); 
            res = stmt.executeQuery(sql); 
            System.out.println("执行“select * query”运行结果:"); 
            while (res.next()) { 
                System.out.println(res.getInt(1) + "\t" + res.getString(2)); 
            } 

            // 执行“regular hive query”操作 
            sql = "select count(1) from " + tableName; 
            System.out.println("Running:" + sql); 
            res = stmt.executeQuery(sql); 
            System.out.println("执行“regular hive query”运行结果:"); 
            while (res.next()) { 
                System.out.println(res.getString(1)); 

            } 

            conn.close(); 
            conn = null; 
        } catch (ClassNotFoundException e) { 
            e.printStackTrace(); 
            log.error(driverName + " not found!", e); 
            System.exit(1); 
        } catch (SQLException e) { 
            e.printStackTrace(); 
            log.error("Connection error!", e); 
            System.exit(1); 
        } 

    } 
}