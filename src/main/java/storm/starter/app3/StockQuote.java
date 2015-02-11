package storm.starter.app3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
/**
 * 单只股票行情
 * @author Mervin
 *
 */
public class StockQuote
{
	public static final int DATA_INDEX_NOTSET		= -1;
	public static final int DATA_INDEX_OPEN			= 0; //收盘价
	public static final int DATA_INDEX_CLOSE		= 1; //开盘价
	public static final int DATA_INDEX_HIGH			= 2; //最高价
	public static final int DATA_INDEX_LOW			= 3; //收盘价
	public static final int DATA_INDEX_VOL			= 4; //成交量
	public static final int DATA_INDEX_AMOUNT		= 5; //成交额
	
	public static final int DATA_INDEX_LNBHD		= 6; //量能饱和度
	public static final int DATA_INDEX_LN			= 7; //量能
	public static final int DATA_INDEX_LNZF			= 8; //量能正负
	public static final int DATA_INDEX_VA1			= 9; //VA1
	public static final int DATA_INDEX_VA2_TMP_1	= 10; //VA2
	public static final int DATA_INDEX_VA2			= 11; //VA2
	public static final int DATA_INDEX_QG			= 12; //前高 
	public static final int DATA_INDEX_VA3			= 13; //VA3
	public static final int DATA_INDEX_VA4_TMP_1	= 14; //VA4
	public static final int DATA_INDEX_VA4			= 15; //VA4
	public static final int DATA_INDEX_QD			= 16; //前低 

	public static final int DATA_INDEX_MAX			= 20; //指数最大序号
	
	public static final int DATA_INDEX_TEMP_1		= DATA_INDEX_MAX + 1;
	public static final int DATA_INDEX_TEMP_2		= DATA_INDEX_MAX + 2;
	public static final int DATA_INDEX_TEMP_3		= DATA_INDEX_MAX + 3;
	
	public static final int DATA_INDEX_TEMP_MAX		= DATA_INDEX_MAX + 3;
		
	public class StockQuoteData
	{
		public int date; //yyyymmdd
		public int time; //hhmmss
		public double[] var = new double[DATA_INDEX_TEMP_MAX + 1];
	}
	
	public interface ICondition
	{
		public boolean check(Object context, Object arg);
	}
	
	public String code; //股票代码
	public String name; //股票名称
	public StockQuoteData[] quotes; //时点行情
	
	/**
	 * 构造函数
	 */
	public StockQuote()
	{
	}
	
	public static double max(double v1, double v2)
	{
		return v1 >= v2 ? v1 : v2;
	}
	
	public static int max(int v1, int v2)
	{
		return v1 >= v2 ? v1 : v2;
	}
	
	/**
	 * HHV(X,N) 求N周期内X最高值，如果N=0，则从第一个值开始
	 * @param date_index 当前日期序号, [date_index - N + 1, date_index]
	 * @param data_index
	 * @param cycle 周期N
	 * @return
	 */
	public double HHV(int date_index, int data_index, int cycle)
	{
		int date_begin;
		double v, v2;
		
		if(cycle == 0)
			date_begin = 0;
		else
			date_begin = max(date_index - cycle + 1, 0);
		
		v = quotes[date_begin].var[data_index];
		for(int i=date_begin + 1; i<= date_index; i++)
		{
			v2 = quotes[i].var[data_index];
			if(v < v2)
				v = v2;
		}
		
		return v;
	}
	
	/**
	 * EMA(X,N) X的N日指数移动平均
	 * @param date_index 当前日期序号
	 * @param data_index
	 * @param cycle 周期N
	 * @return
	 */
	public double EMA(int date_index, int data_index, int cycle)
	{
		int date_begin;
		double alpha = 2.0f / (cycle + 1);
		double v1 = 0, v2 = 0;
		double k = 1; 

		cycle = (int)(3.45 * (cycle + 1)) + 1;
		date_begin = max(date_index - cycle + 1, 0);
		v1 = quotes[date_index].var[data_index];
		v2 = 1;
		for(int i=date_index - 1; i>=date_begin + 1; i--)
		{
			k *= 1 - alpha;
			
			v1 += k * quotes[i].var[data_index];
			v2 += k;
		}
		
		return v1 / v2;
	}
	
	/**
	 * BARSLAST(X) 上一次X不为0到现在的天数
	 * @param date_index 当前日期序号
	 * @param cond 条件判断, cond.check(date index, arg)
	 * @param arg 参数
	 * @return
	 */
	public int BARSLAST(int date_index, ICondition cond, Object arg)
	{
		for(int i=date_index; i >=0 ;i--)
		{
			if(cond.check(i, arg))
				return date_index - i;
		}
		return date_index;
	}
	
	/**
	 * HHVBARS(X,N) 求N周期内X最高值到当前的周期数，N=0从第一个有效值开始统计
	 * @param date_index
	 * @param data_index
	 * @return
	 */
	public int HHVBARS(int date_index, int data_index, int cycle)
	{
		int date_begin, date_match;
		double v, v2;
		
		if(cycle == 0)
			date_begin = 0;
		else
			date_begin = max(date_index - cycle + 1, 0);
		
		date_match = date_begin;
		v = quotes[date_begin].var[data_index];
		for(int i=date_begin + 1; i<= date_index; i++)
		{
			v2 = quotes[i].var[data_index];
			if(v < v2)
			{
				date_match = i;
				v = v2;
			}
		}
		
		return date_index - date_match;
	}
	
	/**
	 * LLVBARS(X,N) 求N周期内X最低值到当前的周期数，N=0从第一个有效值开始统计
	 * @param date_index
	 * @param data_index
	 * @return
	 */
	public int LLVBARS(int date_index, int data_index, int cycle)
	{
		int date_begin, date_match;
		double v, v2;
		
		if(cycle == 0)
			date_begin = 0;
		else
			date_begin = max(date_index - cycle + 1, 0);
		date_match = date_begin;
		v = quotes[date_begin].var[data_index];
		for(int i=date_begin + 1; i<= date_index; i++)
		{
			v2 = quotes[i].var[data_index];
			if(v > v2)
			{
				date_match = i;
				v = v2;
			}
		}
		
		return date_index - date_match;
	}
	
	/**
	 * CROSS(A,B) 如果A从下向上穿过B，返回true
	 * @param date_index
	 * @param data_index_a
	 * @param data_a
	 * @param data_index_b
	 * @param data_b
	 * @return
	 */
	public int CROSS(int date_index, int data_index_a, double data_a, int data_index_b, double data_b)
	{
		double prev_data_a = data_a;
		double prev_data_b = data_b;
		
		if(date_index == 0)
			return 0;
		
		//上一周期的值
		if(data_index_a != DATA_INDEX_NOTSET)
			prev_data_a = quotes[date_index - 1].var[data_index_a];
		if(data_index_b != DATA_INDEX_NOTSET)
			prev_data_b = quotes[date_index - 1].var[data_index_b];
		
		//本周期的值
		if(data_index_a != DATA_INDEX_NOTSET)
			data_a = quotes[date_index].var[data_index_a];
		if(data_index_b != DATA_INDEX_NOTSET)
			data_b = quotes[date_index].var[data_index_b];
		
		return prev_data_a < prev_data_b && data_a > data_b ? 1 : 0;
	}
	
	/**
	 * BACKSET(X, N) 若X非0，则将当前到前N周期的值置为1
	 * @param date_index
	 * @param data_index
	 * @param cycle
	 * @param cond
	 * @param arg
	 * @return
	 */
	public void BACKSET(int date_index, int data_index, int cycle, ICondition cond, Object arg)
	{
		if(cond.check(date_index, arg))
		{
			for(int i=date_index; i >=max(date_index - cycle + 1, 0) ;i--)
			{
				quotes[i].var[data_index] = 1;
			//	else
			//		quotes[i].var[data_index] = 0;
			}
		}
	}
	
	/**
	 * REF(X, A) 引用A周期前的X值
	 * @param date_index
	 * @param data_index
	 * @param cycle
	 * @return
	 */
	public double REF(int date_index, int data_index, int cycle)
	{
		date_index = max(date_index - cycle, 0);
		return quotes[date_index].var[data_index];
	}
	
	public void loadStockQuote(Connection conn, String code) throws SQLException
	{
		PreparedStatement stmt;
		ResultSet rs;
		ArrayList<StockQuoteData> list = new ArrayList<StockQuote.StockQuoteData>();
//		String sql = " select a.trade_day, a.open_price, a.close_price, a.high_price, a.low_price, a.vol, a.val from t_stock_daily_quote a "
//				+ " where a.trade_code = ?"
//				+ " order by a.trade_day";
		String sql = " select a.data_dt, a.open_price, a.close_price, a.high_price, a.low_price, a.turnover_vol, a.turnover_val,a.trade_abbr from ods.stock_daily_quote a "
				+ " where a.trade_code = ? and a.trade_state = 1"
				+ " order by a.data_dt";
		stmt = conn.prepareStatement(sql);
		stmt.setString(1, code);
		rs = stmt.executeQuery();
		while(rs.next())
		{
			StockQuoteData data = new StockQuoteData();
			data.date = rs.getInt(1);
			data.time = 0;
			data.var[DATA_INDEX_OPEN] = rs.getDouble(2);
			data.var[DATA_INDEX_CLOSE] = rs.getDouble(3);
			data.var[DATA_INDEX_HIGH] = rs.getDouble(4);
			data.var[DATA_INDEX_LOW] = rs.getDouble(5);
			data.var[DATA_INDEX_VOL] = rs.getDouble(6);
			data.var[DATA_INDEX_AMOUNT] = rs.getDouble(7);
			list.add(data);
			this.code = code;
			this.name = rs.getString(8);
		}
		rs.close();
		stmt.close();
		
		this.quotes = list.toArray(new StockQuoteData[0]);
	}
	 
	public static void main(String[] args)
	{
		Connection conn = null;
		StockQuote stock = new StockQuote();
		ArrayList<String> codeList = new ArrayList<String>();
		
		try
		{
			Class.forName("org.postgresql.Driver").newInstance();
			//conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/security", "postgres" , "postgres");
			conn = DriverManager.getConnection("jdbc:postgresql://192.168.66.19:5432/finanalyze", "finana", "finana");

			String sql = " select trade_code "
					+ " from ods.stock_daily_quote"
					+ " where data_dt = '20141201'"
					+ " ORDER BY trade_code";
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while(rs.next())
			{
				codeList.add(rs.getString(1));
			}
			rs.close();
			stmt.close();
			
			for(String code : codeList)
			{
						stock.loadStockQuote(conn, code);
						System.out.println(stock.code+","+stock.name+","+stock.EMA(100,0,12));
//						for(int i=0; i<stock.quotes.length; i++)
//						{
//							StockQuoteData quote = stock.quotes[i];
//							if(quote.date >= 20140300 && quote.date < 20150800)
//							{
//								System.out.println(JSON.toJSONString(quote));
//								System.out.println("" + quote.date + ":量能正负: " + quote.var[DATA_INDEX_LNZF]);
//								System.out.println("" + quote.date + ":前　　高: " + quote.var[DATA_INDEX_QG]);
//								System.out.println("" + quote.date + ":前　　低: " + quote.var[DATA_INDEX_QD]);
//							}
//						}
			 
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(conn != null)
			{
				try
				{
					conn.close();
				}
				catch (SQLException e)
				{
					e.printStackTrace();
				}
			}
		}
		
	}
}
