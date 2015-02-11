package storm.starter.app2;

import redis.clients.jedis.Jedis;


public class TestWorkerThread  implements Runnable {
    private String code;
    private String date;
    public TestWorkerThread(String code,String date){
        this.code=code;
        this.date=date;
    }
    @Override
    public void run() {
    	Jedis jedis = Test.pool.getResource();
    	try{
 			jedis.publish("max-drawdown", code+","+date);
 		}catch(Exception e){
 			e.printStackTrace();
 		}
    	finally{
    		Test.pool.returnResource(jedis);
    	}
    }
}
