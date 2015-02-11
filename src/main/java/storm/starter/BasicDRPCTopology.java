package storm.starter;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology is a basic example of doing distributed RPC on top of Storm. It implements a function that appends a
 * "!" to any string you send the DRPC function.
 * <p/>
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more information on doing distributed RPC on top of
 * Storm.
 */
public class BasicDRPCTopology {
  public static class ExclaimBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String input = tuple.getString(1);
      
      collector.emit(new Values(tuple.getValue(0), md5(input)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "result"));
    }

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
  public static void main(String[] args) throws Exception {
    LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
    builder.addBolt(new ExclaimBolt(), 3);

    Config conf = new Config();

    if (args == null || args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();

      cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));

      for (String word : new String[]{ "hello", "goodbye" }) {
        System.out.println("Result for \"" + word + "\": " + drpc.execute("exclamation", word));
      }

      cluster.shutdown();
      drpc.shutdown();
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
    }
  }
}
