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
	
	public static void main(String[] args)
	{
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String date=null;
		String key=null;
		String dt=null;
		int test_ip_num=0;
		while(true)
		{
			// 每天凌晨 3 点执行
			if(TimeFormatter.getHour().equals("03"))
			{
				//获取实例
				redisserver=RedisServer.getInstance();
				test_ip_num=0;
				logger.info(" Start to get all storm-redis-keys");
				
				date=TimeFormatter.getDate2(); //获取当前日期，需要确定本程序运行的系统环境时间是正确的时间
				keys=redisserver.keys("*"); 		//获取所有的keys

				try {
					logger.info(" Start to clear storm-redis-keys which are out of date!!!");
					keylist = keys.iterator();
					while(keylist.hasNext())
					{
						key=keylist.next().toString();
						if(StringUtils.contains(key, "src_date")==false && StringUtils.contains(key, "dst_date")==false ){
							if(StringUtils.contains(key, date)==false){
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
					
					logger.info(" Complete clear redis-keys, removes "+test_ip_num+" ip (out of date)");					
					Thread.sleep(1000*60*60*22);
				} catch (Exception e) {
					logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
				}
			}else
			{
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
