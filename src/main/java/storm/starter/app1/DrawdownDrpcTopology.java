package storm.starter.app1;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.topology.TopologyBuilder;

public class DrawdownDrpcTopology {
	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();   
		LocalDRPC drpc = new LocalDRPC();
	    DRPCSpout drpcSpout = (args != null && args.length > 0)?new DRPCSpout("max_drawdown_DRPC"):new DRPCSpout("max_drawdown_DRPC",drpc);  
	    builder.setSpout("drpc-input", drpcSpout,25); 
	    builder.setBolt("drawdown", new DrawdownBolt(), 50).noneGrouping("drpc-input");  
	    builder.setBolt("return", new ReturnResults(),50).noneGrouping("drawdown"); 
	    Config conf = new Config();
	    conf.setDebug(false);  
	    if (args != null && args.length > 0) {
		    	 conf.setNumWorkers(3);
		    	 StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
	   }else{
	    		 LocalCluster cluster = new LocalCluster();
	    	      cluster.submitTopology("max_drawdown_DRPC", conf, builder.createTopology());
	    	      String result = drpc.execute("max_drawdown_DRPC", "600000,20141211");
	    	      System.out.println(result);
	    	      cluster.shutdown();
	    	      drpc.shutdown();
	    }
	}

}
