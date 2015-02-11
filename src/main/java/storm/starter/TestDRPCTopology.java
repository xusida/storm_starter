package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.DRPCClient;

public class TestDRPCTopology {
	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();   
	    //开始的Spout  
	    DRPCSpout drpcSpout = new DRPCSpout("md5drpc");  
	    builder.setSpout("drpc-input", drpcSpout,5); 
	    builder.setBolt("md5", new MD5Bolt(), 5).noneGrouping("drpc-input");  
	    builder.setBolt("return", new ReturnResults(),5).noneGrouping("md5");  
	
	    Config conf = new Config();  
	    conf.setDebug(false);  
	    conf.setNumWorkers(3);
	      
	    try  
	    {   
	    	StormSubmitter.submitTopology("md5drpc", conf,builder.createTopology());  
	    	
	    }  
	    catch (Exception e)  
	    {  
	        e.printStackTrace();  
	    }  
	}
}
