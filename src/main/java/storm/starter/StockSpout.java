package storm.starter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StockSpout extends BaseRichSpout{
	 public static Logger LOG = LoggerFactory.getLogger(StockSpout.class);
	 boolean _isDistributed;
	 SpoutOutputCollector _collector;
	 boolean readOver = false;
	 
	 public StockSpout() {
	        this(true);
	 }

	 public StockSpout(boolean isDistributed) {
	        _isDistributed = isDistributed;
	 }
	 
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		 _collector = collector;
	}

	@Override
	public void nextTuple() {
		try{
        Utils.sleep(100);
		// TODO Auto-generated method stub
//        int stockCode = 600000+(int)(Math.random()*300);
//        int stockPrice = (int)(Math.random()*100);
//		 _collector.emit(new Values(stockCode+"",stockPrice));
        if(!readOver){
	        Properties p = new Properties();
	        p.load(StockSpout.class.getResourceAsStream("stock.properties"));
	        BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(p.getProperty("filename"))));
	        BufferedReader in = new BufferedReader(new InputStreamReader(bis, "utf-8"), 10 * 1024 * 1024);//10M缓存
	        while (in.ready()) {
	            String line = in.readLine();
	            _collector.emit(new Values(line.split(",")[0],Integer.parseInt(line.split(",")[1])));
	        }
	        in.close();
        }
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			readOver = true;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("code","price"));
		
	}
	
}
