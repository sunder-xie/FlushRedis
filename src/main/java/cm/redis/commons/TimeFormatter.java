package cm.redis.commons;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

import org.apache.commons.lang.StringUtils;


/**
 * 时间相关的类
 * @author Nick
 *
 */
public class TimeFormatter {

	/**
	 * 获取当前的时间，格式 yyyy-MM-dd HH:mm:ss
	 * @return 当前时间
	 */
	public static String getNow()
	{
		String now="1900-01-01 00:00:00";			
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置日期格式		
		now=df.format(new Date());           // new Date()为获取当前系统时间		
		return now;
	}
	
	/**
	 * 获取当前的时间，格式  yyyyMMddHHmmss
	 * @return 当前时间
	 */
	public static String getNow2()
	{
		String now="19000101000000";			
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss"); //设置日期格式		
		now=df.format(new Date());           // new Date()为获取当前系统时间		
		return now;
	}
	
	/**
	 * 获取当前的月份，格式  yyyyMM
	 * @return 当前月份
	 */
	public static String getMonth()
	{
		String dt="190001";			
		SimpleDateFormat df = new SimpleDateFormat("yyyyMM"); //设置日期格式		
		dt=df.format(new Date());           // new Date()为获取当前系统时间		
		return dt;
	}
	
	/**
	 * 获取当前的上个月份，格式  yyyyMM
	 * @return 当前月份
	 */
	public static String getPreMonth()
	{
		String dt="190001";			
		
		Calendar c = Calendar.getInstance(); 
        c.add(Calendar.MONTH, -1); 
		SimpleDateFormat df = new SimpleDateFormat("yyyyMM"); //设置日期格式		
		dt=df.format(c.getTime());           // new Date()为获取当前系统时间		
		return dt;
	}
	
	/**
	 * 获取当前的日期，格式  yyyyMMdd
	 * @return 当前日期
	 */
	public static String getDate()
	{
		String dt="19000101";			
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd"); //设置日期格式		
		dt=df.format(new Date());           // new Date()为获取当前系统时间		
		return dt;
	}
	
	/**
	 * 获取当前的日期，格式  yyyy-MM-dd
	 * @return 当前日期
	 */
	public static String getDate2()
	{
		String dt="1900-01-01";			
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); //设置日期格式		
		dt=df.format(new Date());           // new Date()为获取当前系统时间		
		return dt;
	}
	
	/**
	 * 获取昨天，格式  yyyyMMdd
	 * @return 昨天
	 */
	public static String getYestoday()
	{
		String dt="190001";			
		
		Calendar c = Calendar.getInstance(); 
        c.add(Calendar.DATE, -1); 
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd"); //设置日期格式		
		dt=df.format(c.getTime());           // new Date()为获取当前系统时间		
		return dt;
	}
	
	/**
	 * 获取昨天，格式  yyyy-MM-dd
	 * @return 昨天
	 */
	public static String getYestoday2()
	{
		String dt="190001";			
		
		Calendar c = Calendar.getInstance(); 
        c.add(Calendar.DATE, -1); 
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); //设置日期格式		
		dt=df.format(c.getTime());           // new Date()为获取当前系统时间		
		return dt;
	}
	
	/**
	 * 获取当前小时，格式  HH
	 * @return 当前小时
	 */
	public static String getHour()
	{
		String now="1900-01-01 00:00:00";			
		SimpleDateFormat df = new SimpleDateFormat("HH"); //设置日期格式		
		now=df.format(new Date());           // new Date()为获取当前系统时间		
		return now;
	}
	
	/**
	 * 获取当前分钟，格式 mm
	 * @return 当前分钟
	 */
	public static String getMinute()
	{
		String now="1900-01-01 00:00:00";			
		SimpleDateFormat df = new SimpleDateFormat("mm"); //设置日期格式		
		now=df.format(new Date());           // new Date()为获取当前系统时间		
		return now;
	}
	
	/**
	 * 将当前实时时间格式从 "英文月份 阿拉伯日 hh:mm:ss"转成"YYYY-MM-DD hh:mm:ss" 注意阿拉伯日如果不足两位DD，则是空格加D即 “ D”的格式
	 * 注意默认日志记录时间允许与当前系统时间有不大于一个月的差异，否则年份将无法测定
	 * @param dt
	 * 2016-01-23添加
	 */
	public static String Tra_realdate1(String dt)
	{
		String logdate="1999-01-01 00:00:00";	
		String[] sep=dt.split(" ");
		if(sep!=null)
		{
			if(sep.length==3||sep.length==4)
			{
				Calendar sysdate=Calendar.getInstance();           //获取当前系统时间
				int year=0;
				int month=0;
				int logmonth=0;  	//记录日志中的对应月份 
				sep[0]=sep[0].toLowerCase();		//小写字母
				year=sysdate.get(Calendar.YEAR);
				month=sysdate.get(Calendar.MONTH)+1;
				if(StringUtils.equals(sep[0], "jan")==true){logmonth=1;sep[0]="01";}
				else if(StringUtils.equals(sep[0], "feb")==true){logmonth=2;sep[0]="02";}
				else if(StringUtils.equals(sep[0], "mar")==true){logmonth=3;sep[0]="03";}
				else if(StringUtils.equals(sep[0], "apr")==true){logmonth=4;sep[0]="04";}
				else if(StringUtils.equals(sep[0], "may")==true){logmonth=5;sep[0]="05";}
				else if(StringUtils.equals(sep[0], "jun")==true){logmonth=6;sep[0]="06";}
				else if(StringUtils.equals(sep[0], "jul")==true){logmonth=7;sep[0]="07";}
				else if(StringUtils.equals(sep[0], "aug")==true){logmonth=8;sep[0]="08";}
				else if(StringUtils.equals(sep[0], "sep")==true){logmonth=9;sep[0]="09";}
				else if(StringUtils.equals(sep[0], "oct")==true){logmonth=10;sep[0]="10";}
				else if(StringUtils.equals(sep[0], "nov")==true){logmonth=11;sep[0]="11";}
				else if(StringUtils.equals(sep[0], "dec")==true){logmonth=12;sep[0]="12";}
				else {logmonth=0;}
				//防止跨年时造成日志与实际日期中的年份错位
				if(month==1&&logmonth==12)
				{
					year=year-1;
				}
				if(logmonth>0)
				{
					if(sep.length==3)logdate=String.valueOf(year)+"-"+sep[0]+"-"+sep[1]+" "+sep[2];
					else logdate=String.valueOf(year)+"-"+sep[0]+"-0"+sep[2]+" "+sep[3];
				}
			}
		}
		return logdate;
	}
	
	/**
	 * 获取同一个日期中的时间跨度
	 * @param start_time
	 * @param end_time
	 * @return 返回格式是hh:mm:ss格式，表示持续时间
	 */
	public static String get_nowaday_length(String start_time, String end_time)
	{
		String timelength="00:00:00";
		SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss"); //设置日期格式
		Date s=null;
		Date e=null;
		long interval=0;
		long sec=0;
		long min=0;
		long hour=0;
		if(start_time.compareTo(end_time)<0)
		{
			try {
				s=df.parse(start_time);
				e=df.parse(end_time);
				
				interval=(e.getTime()-s.getTime())/1000; //获取时间相差毫秒数
				sec=(interval%60);
				interval=interval/60;
				min=interval%60;
				interval=interval/60;
				hour=interval;
				timelength="";
				if(hour<10)timelength="0"+String.valueOf(hour)+":";
				else timelength=String.valueOf(hour)+":";
				if(min<10)timelength+="0"+String.valueOf(min)+":";
				else timelength+=String.valueOf(min)+":";
				if(sec<10)timelength+="0"+String.valueOf(sec);
				else timelength+=String.valueOf(sec);
			} catch (Exception exc) {
				return timelength;
			}
			
		}
		return timelength;
	}
	
	/**
	 * 当前小时，是否有指定范围内
	 * @param start
	 * @param end
	 * @return
	 */
	public static boolean isBetweenHour(int start,int end)
	{
		int now=-1;
		try{
			now=Integer.parseInt(getHour());
		}catch(Exception exc){}
				
		if(now<start || now>end || now<0)
			return false;
		else
			return true;
	}
	
	/**
	 * 当前分钟，是否有指定范围内，用于现场促销，只到10分钟数，如53分钟，则判断50的范围内。如果是现场促销活动，则提前一个小时宣传
	 * @param start
	 * @param end
	 * @return
	 */
	public static boolean isBetweenMinute(String type,int start,int end)
	{
		int now=-1;
		int mi=-1;
		try{
			now=Integer.parseInt(getHour());
			mi=Integer.parseInt(getMinute());
		}catch(Exception exc){}
		
		now=now*10+(int)(mi/10);
		
		start=(int)(start/10);
		end=(int)(end/10);
		
		if(StringUtils.contains(type, "cx"))
		{
			start=start-10;
			
			// 最早 9 点开始
			if(start<90)
				start=90;	
		}
		
		if(now<start || now>end || now<0)
			return false;
		else
			return true;
	}
	
	/**
	 * 随机休眠
	 * @param minue
	 * @param title
	 */
	public static void rndSleep(double minue)
	{
		// 随机休眠
		try{
			int sec = new Random().nextInt((int)(60*minue)) + 1;

			//MyString.printLog("[随机休眠]（"+sec+" 秒）...");
			
			Thread.sleep(sec*1000);
		}catch(Exception exc)
		{
			exc.printStackTrace();
		}
	}
	
	/**
	 * 固定休眠
	 * @param minue
	 * @param title
	 */
	public static void sleep(double minue)
	{
		// 随机休眠
		try{
			int sec = (int)(60*minue);

			//MyString.printLog("[固定时长休眠]（"+sec+" 秒）...");
			
			Thread.sleep(sec*1000);
		}catch(Exception exc)
		{
			exc.printStackTrace();
		}
	}
	
	public static void main(String[] args)throws Exception
	{
		//System.out.println(System.currentTimeMillis()+" , "+(System.currentTimeMillis()%2));
		System.out.println(TimeFormatter.getPreMonth());
		System.out.println(StringUtils.substring("460020", 0, 15));
		//System.out.println(MyString.toMD5("2014-09-30 19:34:59.926000__460000862936335__860465008355330__http://nsclick.baidu.com/v.gif?pid=307&type=3075&l=8026&t=69&s=8026&v=1397&f=7000&r=http://m.baidu.com/from=1328a/s?word=www.12306.cn&sa=ib&ts=7260167&u=http://m.baidu.com/from=1328a/bd_page_type=1/ssid=0/uid=0/pu=usm%400%2csz%401321_1001%2cta%40utouch_2_4.2_3_534/baiduid=742b4deb5c8002f21f8ecae0625631bd/w=0_10_www.12306.cn/t=wap/l=3/tc?ref=www_utouch&lid=11256738092731074070&order=1&vit=osres&tj=www_normal_1_0_10&m=8&dict=22&sec=41843&di=5e34582123ba6d"));
		System.out.println(StringUtils.substring("1234567", 0, 4));
		
		URL url=new URL("http://qyxy.baic.gov.cn");
		System.out.println(url.getHost());
		
		int m=-1;
		try{ m=Integer.parseInt(StringUtils.substring(TimeFormatter.getMinute(), 0, 1));	}catch(Exception exc){}
		System.out.println(m);
		
		// 83526a1549496c5d8a7b6af6f98124
		// fec3d34fa120953cb5b78d294fdbcc66
		// 4b49551e1ae627542cc67ca7f75d99
	}
	
}
