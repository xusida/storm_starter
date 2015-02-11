package storm.starter.app1;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import backtype.storm.utils.DRPCClient;

public class Test {
	public static void main(String[] args){
		try{
			ExecutorService threadPool = Executors.newFixedThreadPool(100);
			long btime = System.currentTimeMillis();
			for(int i=0;i<5000;i++){
				threadPool.submit(new TestWorkerThread(String.valueOf(600000+i)));
			}
			threadPool.shutdown();
			while(!threadPool.isTerminated());
			System.out.println("total cost:" + (System.currentTimeMillis()-btime));
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
}
