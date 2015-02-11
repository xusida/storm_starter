package storm.starter;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.utils.DRPCClient;

public class DRPCTest {
	public class Result{
		String name;
		String md5Value;
	}
	
	public static void main(String[] args) {
		try {
			
			JedisPoolConfig poolconfig = new JedisPoolConfig();
			poolconfig.setMaxTotal(100);
			final JedisPool pool = new JedisPool(poolconfig, "nimbus");
			for (int i = 0; i < 3; i++) {
				new Thread() {
					public void run() {
						Jedis jedis = pool.getResource();
						DRPCClient client = new DRPCClient("nimbus", 3772);
						for (int j = 0; j < 1000; j++) {
							try {
								String name = Thread.currentThread().getName()
										+ "," + j;
								
								System.out
										.println(name + "," + jedis.get(name));
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						pool.returnResource(jedis);

					}
				}.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

}
