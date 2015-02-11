package storm.starter.app2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DrawdownBolt implements IRichBolt {
	private static JedisPool pool = null;
	private OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		JedisPoolConfig poolconfig = new JedisPoolConfig();
		poolconfig.setMaxTotal(100);
		if(pool == null){
			pool = new JedisPool(poolconfig, "nimbus");
		}
	}

	@Override
	public void execute(Tuple input) {
		 String code = input.getString(0);
		 String date = input.getString(1);
		 String maxDrawdown = getMaxDrawdownByDateAndCode(code,date);
	     collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		  declarer.declare(new Fields("result", "return-info"));  

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	public String getMaxDrawdownByDateAndCode(String code,String date){
		String ret = null;
		Jedis jedis = pool.getResource();
		String key = "stock_timeline:"+code+":"+date;
		List<String> s = jedis.lrange(key,0,-1);
		ret = String.valueOf(getMaxDrawdown(s));
		jedis.hset("stock_info:"+code,date,ret);
		pool.returnResource(jedis);
		return ret;
	}
	
	/**
	 * 获取最大回撤值
	 * @param 按时间排序的价格列表
	 * @return
	 */
	public static double getMaxDrawdown(List<String> list){
		double maxDrawdown = 0;//最大回撤值
		double price = 0;//初始化比较价格
		for(String v : list){
			double realtimePrice =  Double.parseDouble(v);//取出实时价格
			double val = price - realtimePrice ;
			if(val < 0)//如果实时价格大于比较价格
				price = realtimePrice;
			else
				if(maxDrawdown < val) maxDrawdown = val;//取最大回撤值
		}
		return maxDrawdown;
	}
	
	public static void main(String[] args){
		long ss = System.currentTimeMillis();
		List<String> s = new ArrayList<String>();
		s.add("20");
		s.add("4");
		s.add("24");
		s.add("29");
		s.add("18");
		s.add("24");
		s.add("4");
		s.add("26");
		System.out.println("result:"+getMaxDrawdown(s)+",cost:"+(System.currentTimeMillis()-ss)+"ms");

	}
	
}