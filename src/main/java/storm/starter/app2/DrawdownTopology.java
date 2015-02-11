package storm.starter.app2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class DrawdownTopology {
	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();

	    builder.setSpout("redis-input", new RedisSpout("nimbus",6379,"max-drawdown"),1);
	    builder.setBolt("max-drawdown", new DrawdownBolt(), 10).fieldsGrouping("redis-input",new Fields("code"));

	    Config conf = new Config();
	    conf.setDebug(false);

	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);
	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	    else {
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("max-drawdown", conf, builder.createTopology());
	      //Utils.sleep(10000);
	      //cluster.killTopology("max-drawdown");
	      //cluster.shutdown();
	    }
	}
}
