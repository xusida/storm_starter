package storm.starter;

import java.io.*;

public class WriterFile {

	public static void main1(String[] args){
		String filename = null;
		try{
		  if(args.length ==0){
			  filename = "d:\\stock.txt";
		  }
		  FileWriter fw = new FileWriter(filename);
		  while(fw != null){
			  int stockCode = 600000+(int)(Math.random()*100);
		      int stockPrice = (int)(Math.random()*100);
	          fw.append(stockCode+","+stockPrice+"\n");
	          fw.flush();
		  }
          fw.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
