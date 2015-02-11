package storm.starter;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MD5Bolt implements IRichBolt {

	private OutputCollector collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		 String md5Value = md5(input.getString(0));
		 Object retInfo = input.getValue(1); 
	     collector.emit(new Values(md5Value,retInfo));
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
	
	/**
	 * 使用md5的算法进行加密
	 */
	public static String md5(String plainText) {
		byte[] secretBytes = null;
		try {
			secretBytes = MessageDigest.getInstance("md5").digest(
					plainText.getBytes());
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("没有md5这个算法！");
		}
		String md5code = new BigInteger(1, secretBytes).toString(16);// 16进制数字
		// 如果生成数字未满32位，需要前面补0
		for (int i = 0; i < 32 - md5code.length(); i++) {
			md5code = "0" + md5code;
		}
		return md5code;
	}

}
