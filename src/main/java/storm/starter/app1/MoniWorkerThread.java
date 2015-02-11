package storm.starter.app1;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MoniWorkerThread  implements Runnable {
    private String code;
    private JedisPool pool;
    public MoniWorkerThread(String code,JedisPool pool){
        this.code=code;
        this.pool=pool;
    }
    @Override
    public void run() {
    	 Jedis jedis = pool.getResource();
		 Calendar c = Calendar.getInstance();
		 c.set(2014, 10, 11, 9, 0, 0);
		 for(int j=0;j<4800;j++){
			c.add(Calendar.SECOND, 3);
			String date = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
			Random rand = new Random();
			float price = rand.nextFloat() * 100;
			jedis.lpush("stock_timeline:"+code+":"+date, String.format("%.2f", price));
		}
		pool.returnResource(jedis);
    }
}
