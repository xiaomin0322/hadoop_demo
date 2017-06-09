package zzm.spark.streaming.rocketmq;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * 业务监听实现Demo
 * Created by zzm 
 * 1.先初始化 .     定时轮回  2.start  store restart 3.stop
 * 
 */
public class SparkRocketMqReceiver2 extends Receiver<String> {
	
	 Consumer consumer = null;

	 
    public SparkRocketMqReceiver2() {
    	 super(StorageLevel.MEMORY_AND_DISK_2());  
    	 System.out.println("SparkRocketMqReceiver2 >>>>>>>>>>>>>>>>>>>.初始化了");
	}

    public  static  final  int batcheSize = 100000;
   
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	 private void receive(Consumer consumer) {  
	    	int storeSize=0;
	    	List<String> storeList = new ArrayList<String>();
	    	 System.out.println("队列大小>>>>>>>>>>>>>>>>>>>>>"+consumer.QUEUE.size());
	    	try{
	    		 while (consumer !=null && !consumer.QUEUE.isEmpty() && storeSize < batcheSize) {
	    	        	//非阻塞获取队列元素,取不到时返回null
	    	        	String str = consumer.QUEUE.poll();
	    	        	if(str==null){
	    	        		break;
	    	        	}else{
	    	        		//store(str);
	    	        		storeList.add(str);
	    	        	}
	    	        	storeSize+=1;
	    	        }
	    		 System.out.println("获取数据>>>>>>>>>>>>>>>>>>>>>"+storeSize);
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}finally{
	    		 store(storeList.iterator());
	    		 restart("Trying to connect again");  
	    	}
	    }
   
	@Override
	public void onStart() {
		System.out.println("onStart>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		if(consumer==null){
			consumer = new Consumer();
		}
		// TODO Auto-generated method stub
		   new Thread()  {  
			      @Override public void run() {  
			        receive(consumer);  
			      }  
			    }.start(); 
	}

	@Override
	public void onStop() {
		System.out.println("onStop>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		// TODO Auto-generated method stub
		if(consumer!=null){
			//consumer.stop();
			//consumer = null;
		}
	}
    
}
