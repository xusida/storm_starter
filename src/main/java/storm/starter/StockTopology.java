package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class StockTopology {
	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();

	    builder.setSpout("stock", new StockSpout(),1);
	    builder.setBolt("alert", new StockBolt(), 10).fieldsGrouping("stock",new Fields("code"));

	    Config conf = new Config();
	    conf.setDebug(true);

	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);

	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	    else {

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("stock", conf, builder.createTopology());
	      //Utils.sleep(10000);
	      //cluster.killTopology("stock");
	      //cluster.shutdown();
	    }
	}

}
