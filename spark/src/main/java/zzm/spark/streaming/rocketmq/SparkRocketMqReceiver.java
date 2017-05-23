package zzm.spark.streaming.rocketmq;

import java.util.ArrayList;
import java.util.List;

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

    public  static  final List<String> list = new ArrayList<String>();
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    
    private void receive() {  
    	synchronized (list) {
    		    List<String> store = new ArrayList<String>(list);
    		    System.out.println("store>>>>>>>>>>>>>>>>>>>>>>>"+store.size());
				store(store.iterator());
				list.clear();
				// Restart in an attempt to connect again when server is active again  
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
