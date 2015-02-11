package storm.starter.app2;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import storm.starter.app2.TestWorkerThread;

public class Test {
	//定义股票数量
	public static int STOCK_NUMBER = 5000;
	
	public static JedisPool pool = getJedis();
	
	public static JedisPool getJedis(){
		if(pool == null){
			JedisPoolConfig poolconfig = new JedisPoolConfig();
			poolconfig.setMaxTotal(10);
			pool = new JedisPool(poolconfig, "nimbus");
		}
		return pool;
	}
	
	
	
	
	
	
	public static void main(String[] args){
		try{
			ExecutorService threadPool = Executors.newFixedThreadPool(100);
			long btime = System.currentTimeMillis();
			for(int i=0;i<500;i++){
				threadPool.submit(new TestWorkerThread(String.valueOf(600000+i),"20141111"));
			}
			threadPool.shutdown();
			while(!threadPool.isTerminated());
			System.out.println("total cost:" + (System.currentTimeMillis()-btime));
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
