package zzm.spark.streaming.rocketmq;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.derby.tools.sysinfo;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 顺序消息消费，带事务方式（应用可控制Offset什么时候提交）
 */
public class Consumer implements Serializable{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Consumer(){
		try {
			System.out.println("Consumer init ?>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			init();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new Consumer();
	}
	
	//当spark straming 消费过慢，QUEUE 可能会内存爆掉，建议控制QUEUE大小，控制内存
	public   ConcurrentLinkedQueue<String> QUEUE = new ConcurrentLinkedQueue<String>();
	 DefaultMQPushConsumer consumer = null;
    public  void init() throws MQClientException {
		 consumer = new DefaultMQPushConsumer("MyConsumerGroup");
        consumer.setNamesrvAddr("rocket1.cmall.com:9876");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TopicTest1", "*");
        
        consumer.setConsumeThreadMax(4);
        consumer.setConsumeThreadMin(4);

        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
               // System.out.println(Thread.currentThread().getName() + " Receive New Messages: QUEUE.size===" +QUEUE.size());
                for (MessageExt msg: msgs) {
                    //System.out.println(msg + ", content:" + new String(msg.getBody()));
                	QUEUE.offer(new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }
    
    public void stop(){
    	consumer.shutdown();
    }

}