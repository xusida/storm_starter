package storm.starter.app1;

import backtype.storm.utils.DRPCClient;

public class TestWorkerThread  implements Runnable {
    private String code;
    public TestWorkerThread(String code){
        this.code=code;
    }
    @Override
    public void run() {
    	DRPCClient client = new DRPCClient("nimbus", 3772);
    	try{
 			String ret =  client.execute("max_drawdown_DRPC", code+",20141211");
 			//System.out.println(ret);
 		}catch(Exception e){
 			e.printStackTrace();
 		}finally{
		 if(client != null)client.close();
 		}
    }
}
