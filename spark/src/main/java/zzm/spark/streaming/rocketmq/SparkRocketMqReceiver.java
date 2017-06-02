package zzm.spark.streaming.rocketmq;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * 业务监听实现Demo
 * Created by zzm 
 */
public class SparkRocketMqReceiver extends Receiver<String> {
	
    public SparkRocketMqReceiver() {
    	 super(StorageLevel.MEMORY_AND_DISK_2());  
	}

    public  static  final  int batcheSize = 100000;
    public  static  final  int queueSize  = 100000;
    public  static  final  BlockingQueue<String> QUEUE = new ArrayBlockingQueue<String>(queueSize);
    
    //public  static  final  ConcurrentLinkedQueue<String> list = new ConcurrentLinkedQueue<String>();
   
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void addQueue(String str){
		try {
			//阻塞添加元素
			QUEUE.put(str);
		}catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
    
    private void receive() {  
    	/*synchronized (list) {
    		    List<String> store = new ArrayList<String>(list);
    		    //System.out.println("store>>>>>>>>>>>>>>>>>>>>>>>"+store.size());
				store(store.iterator());
				list.clear();
				// Restart in an attempt to connect again when server is active again  
			    restart("Trying to connect again");  
		}*/
    	
    	int storeSize=0;
    	try{
    		 while (!QUEUE.isEmpty() && storeSize < batcheSize) {
    	        	//非阻塞获取队列元素,取不到时返回null
    	        	String str = QUEUE.poll();
    	        	if(str==null){
    	        		break;
    	        	}else{
    	        		store(str);
    	        	}
    	        	storeSize+=1;
    	        }
    		 System.out.println("获取数据>>>>>>>>>>>>>>>>>>>>>"+storeSize);
    	}catch(Exception e){
    		e.printStackTrace();
    	}finally{
    		 restart("Trying to connect again");  
    	}
    }
	@Override
	public void onStart() {
		// TODO Auto-generated method stub
		   new Thread()  {  
			      @Override public void run() {  
			        receive();  
			      }  
			    }.start(); 
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub
		
	}
    
}
