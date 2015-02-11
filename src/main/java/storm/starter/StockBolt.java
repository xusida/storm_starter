package storm.starter;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class StockBolt extends BaseRichBolt {
	Map<String, Integer> stockMap = new HashMap<String, Integer>();
	OutputCollector _collector;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		  _collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String alertMessage = "";
		String code = tuple.getString(0);
		int price = tuple.getInteger(1);
		if(stockMap.get(code) == null){
			stockMap.put(code, price);
		}else{
			int offPrice = price - stockMap.get(code).intValue();
			alertMessage = code+",["+stockMap.get(code).intValue()+","+price+"]";

			if(offPrice>=20)//上浮动20
				alertMessage = alertMessage+",上涨超20";
			if(offPrice<=-20)//下浮动20
				alertMessage = alertMessage+",下跌超20";
			//设置新价格
			stockMap.put(code, price);
		}
		Utils.sleep(500);
		_collector.emit(tuple, new Values(alertMessage));
	    _collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		 declarer.declare(new Fields("alert_message"));
	}
	
	

}
