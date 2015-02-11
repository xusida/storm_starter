package storm.starter.app2;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RedisSpout implements IRichSpout  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2273741853436125664L;
	SpoutOutputCollector _collector;
	final String host;
	final int port;
	final String pattern;
	LinkedBlockingQueue<String> queue;
	JedisPool pool;
	
	public RedisSpout(String host, int port, String pattern) {
		this.host = host;
		this.port = port;
		this.pattern = pattern;
	}
	
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
			} finally {
				pool.returnResource(jedis);
			}
		}
	};
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		queue = new LinkedBlockingQueue<String>(1000);
		pool = new JedisPool(new JedisPoolConfig(),host,port);
		ListenerThread listener = new ListenerThread(queue,pool,pattern);
		listener.start();
		
	}

	public void close() {
		pool.destroy();
	}

	public void nextTuple() {
		String ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
        	String code = ret.split(",")[0];
        	String date = ret.split(",")[1];
            _collector.emit(new Values(code,date));            
        }
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("code","date"));
	}

	public boolean isDistributed() {
		return false;
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
