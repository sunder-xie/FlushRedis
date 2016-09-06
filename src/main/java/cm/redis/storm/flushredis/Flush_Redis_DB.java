package cm.redis.storm.flushredis;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cm.redis.commons.RedisServer;
import cm.redis.commons.TimeFormatter;

/**
 * 用于删除redis中过期的数据
 * @author nicolashsu 2015-02-16
 *
 */
public class Flush_Redis_DB {
	/*
	 * 对redis中关于storm存放在redis上的数据进行统一的清理工作
	 */
	public static Logger logger=Logger.getLogger(Flush_Redis_DB.class);
	
	//对大日志的数据进行清理
	public static void flush_biglogs(){
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String date=null;
		String key=null;
		String dt=null;
		int test_ip_num=0;
		
		//获取实例
		redisserver=RedisServer.getInstance();

		test_ip_num=0;
		logger.info(" Start to get biglogs storm-redis-keys");
		
		date=TimeFormatter.getDate2(); //获取当前日期，需要确定本程序运行的系统环境时间是正确的时间
		keys=redisserver.keys("*"); 		//获取所有的keys

		//仅删除大日志相关的数据
		try {
			logger.info(" Start to clear biglogs storm-redis-keys which are out of date!!!");
			keylist = keys.iterator();
			while(keylist.hasNext())
			{
				key=keylist.next().toString();
				if(StringUtils.contains(key, "src_date")==false && StringUtils.contains(key, "dst_date")==false ){
					if(StringUtils.contains(key, date)==false&&
					(StringUtils.startsWith(key, "src")==true||StringUtils.startsWith(key, "dst")==true)){
						redisserver.del(key);
						test_ip_num+=1;
					}
				}
				else{
					dt=redisserver.get(key);
					if(dt!=null)
					{
						if(StringUtils.equals(dt, date)==false){
							redisserver.del(key);
							test_ip_num+=1;
						}
					}
				}
			}

			test_ip_num=test_ip_num/5;
			
			//释放内存
			redisserver=null;
			date=null;
			keys=null;
			keylist=null;
			key=null;
			dt=null;
			
			logger.info(" Complete clear biglogs redis-keys, removes "+test_ip_num+" ip (out of date)");					
			Thread.sleep(1000*60*60*11);
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
		}
	}
	
	//对4G维表数据进行清理
	public static void flush_g4jk_ref(){
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String key=null;
		int test_ip_num=0;
		
		//获取实例
		redisserver=RedisServer.getInstance();

		test_ip_num=0;
		logger.info(" Start to get g4jk_ref storm-redis-keys");
		
		keys=redisserver.keys("ref*"); 		//获取所有的ref 相关的keys

		try {
			logger.info(" Start to clear g4jk_ref storm-redis-keys which are out of date!!!");
			keylist = keys.iterator();
			while(keylist.hasNext())
			{
				key=keylist.next().toString();
				redisserver.del(key);
				test_ip_num+=1;
			}
			
			//释放内存
			redisserver=null;
			keys=null;
			keylist=null;
			key=null;
			
			logger.info(" Complete clear g4jk_ref redis-keys, removes "+test_ip_num+" ip (out of date)");					
			Thread.sleep(1000*60*60*11);
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
		}
	}
	public static void main(String[] args)
	{
		while(true)
		{
			if(TimeFormatter.getHour().equals("02")==true||TimeFormatter.getHour().equals("14")==true)
			{
				// 每天凌晨 2 点与下午14点执行，负责清理大日志过期的数据信息
				Flush_Redis_DB.flush_biglogs();
			}else if(TimeFormatter.getHour().equals("03")==true){
				// 每天凌晨 3 点清理维表，重新添加维表信息
				//先获取接口数据，接口数据能够成功获取，再考虑更新
				
				//能够准确获取文件，再进行更新
				Flush_Redis_DB.flush_g4jk_ref();
			}
			else	{
				try{					
					logger.info(" Storm-redis-cleaner is sleeping...zzz...");
					Thread.sleep(1000*60*30);
				}catch(Exception e)
				{
					logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
				}
			}
		}
	}
}
