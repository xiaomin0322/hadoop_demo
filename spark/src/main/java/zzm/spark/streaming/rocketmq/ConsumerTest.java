package zzm.spark.streaming.rocketmq;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ConsumerTest {

	public static ApplicationContext context = null;
	static {
		context = new ClassPathXmlApplicationContext(
				new String[] { "classpath:spark-rocketmq-consumer.xml" });
	}

	public static void main(String[] args) throws Exception {
		System.in.read();
	}

}
