package storm.starter.app3;

import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

public class Test {
	public static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>(3);
	public static JedisPool pool = getJedis();
	public static JedisPool getJedis(){
		if(pool == null){
			JedisPoolConfig poolconfig = new JedisPoolConfig();
			pool = new JedisPool(poolconfig, "nimbus");
		}
		return pool;
	}
	
	
	
	public static void main(String[] args){
		class ListenerThread extends Thread {
			LinkedBlockingQueue<String> queue;
			JedisPool pool;
			String pattern;
			
			public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String pattern) {
				this.queue = queue;
				this.pool = pool;
				this.pattern = pattern;
			}
			
			public void run() {
				
				JedisPubSub listener = new JedisPubSub() {

					@Override
					public void onMessage(String channel, String message) {
						try {
							queue.put(message);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					@Override
					public void onPMessage(String pattern, String channel, String message) {
						try {
							queue.put(message);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					@Override
					public void onPSubscribe(String channel, int subscribedChannels) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public void onPUnsubscribe(String channel, int subscribedChannels) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public void onSubscribe(String channel, int subscribedChannels) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public void onUnsubscribe(String channel, int subscribedChannels) {
						// TODO Auto-generated method stub
						
					}
				};
				
				Jedis jedis = pool.getResource();
				try {
					jedis.psubscribe(listener, pattern);
					
				} catch(Exception e){
					e.printStackTrace();
				}finally {
					pool.returnResource(jedis);
				}
			}
		};
		
		
		try{
			
			ListenerThread listener = new ListenerThread(queue,pool,"max-drawdown");
			listener.start();
			new Thread(){
				public void run(){
					while(queue!=null){
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						String ret = queue.poll();
						if(ret !=null)
						System.out.println(ret);
					}
				}
			}.start();
			
		
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
