package zzm.spark.streaming.rocketmq;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.cmall.mq.rocket.listener.RocketMqMessageListener;

/**
 * 业务监听实现Demo
 * Created by zzm 
 */
public class SparkRocketMqListenerReceiver implements RocketMqMessageListener {
	
	public boolean onMessage(List<MessageExt> messages, ConsumeConcurrentlyContext Context) {
        for (int i = 0; i < messages.size(); i++) {
            Message msg = messages.get(i);
            try{
            	  String msgStr = new String(msg.getBody(),"utf-8");
            	  synchronized (SparkRocketMqReceiver.list) {
            		  System.out.println("msgStr : "+msgStr);
            		  SparkRocketMqReceiver.list.add(msgStr);
            	  }
			}catch(Exception e){
				e.printStackTrace();
			}
        }
        return true;
    }
  
}
