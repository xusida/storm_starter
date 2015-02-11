package storm.starter.app1;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MoniData {
	//定义股票数量
	public static int STOCK_NUMBER = 5000;
	
	public static JedisPool pool;
	
	public static void initRedis(){
		JedisPoolConfig poolconfig = new JedisPoolConfig();
		poolconfig.setMaxTotal(100);
		pool = new JedisPool(poolconfig, "nimbus");
	}
	
	public static void moniStockInfo(){
		Jedis jedis = pool.getResource();
		for(int i=0;i<STOCK_NUMBER;i++){
			int code = 600000+i;
			jedis.hset("stock_info:"+code, "code", String.valueOf(code));
			jedis.hset("stock_info:"+code, "market", "SH");
		}
	}
	
	public static void clear(){
		Jedis jedis = pool.getResource();
		for(int i=0;i<STOCK_NUMBER;i++){
			int code = 600000+i;
			jedis.hdel("stock_info:"+code, "20141111");
		
		}
	}
	
	public static void moniStockTimelineInfo(){
		long btime = System.currentTimeMillis();
		ExecutorService threadPool = Executors.newFixedThreadPool(500);
		for(int i=0;i<STOCK_NUMBER;i++){
			final String code = String.valueOf(600000+i);
			MoniWorkerThread worker = new MoniWorkerThread(code,pool);
			threadPool.submit(worker);
		}
		while(!threadPool.isTerminated());
		System.out.println("total moniStockTimelineInfo cost:" + (System.currentTimeMillis()-btime));
	}
	
	public static void main(String[] args){
		try{
			long btime = System.currentTimeMillis();
			initRedis();
			clear();
			//moniStockInfo();
			//moniStockTimelineInfo();
			System.out.println("cost:"+(System.currentTimeMillis()-btime));
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
