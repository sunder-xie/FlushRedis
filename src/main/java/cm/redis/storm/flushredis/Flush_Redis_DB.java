package cm.redis.storm.flushredis;

import java.util.Iterator;
import java.util.HashMap;
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
		TreeSet<String> srckeys=null; 
		TreeSet<String> dstkeys=null;
		TreeSet<String> ipset=null;
		Iterator<String> tsit =null;
		Iterator<String> hsit =null;
		String key=null;
		String dt=null;
		String ip=null;
		int test_ip_num=0;
		int size=0;
		HashMap<String,TreeSet<String>> hashmap=null;	
		while(true)
		{
			// 每天凌晨 3 点执行
			if(TimeFormatter.getHour().equals("03"))
			{
				//获取实例
				redisserver=RedisServer.getInstance();
				hashmap=new HashMap<String,TreeSet<String>>();
				test_ip_num=0;
				size=0;
				logger.info(" Start to get all storm-redis-keys");
				srckeys=redisserver.keys("src_date*"); 
				dstkeys=redisserver.keys("dst_date*");
				try {
					logger.info(" Start to clear storm-redis-keys which are out of date!!!");
					tsit = srckeys.iterator();
					while(tsit.hasNext())
					{
						key=tsit.next().toString();
						ip=StringUtils.substringAfterLast(key, "_");	//获取key中的ip信息
						dt=redisserver.get(key);
						if(hashmap.containsKey(dt)==false)
						{
							ipset=new TreeSet<String>();
						}
						else{
							ipset=hashmap.get(dt);
						}
						ipset.add(ip);
						hashmap.put(dt, ipset);
					}
					
					if(hashmap.isEmpty()==false){
						ipset=new TreeSet<String>();
						ipset.addAll(hashmap.keySet());
						size=ipset.size()-1;
						hsit=ipset.iterator();
						for(int i=0;i<size;i++)
						{
							key=hsit.next().toString();//按照日期，除了最后一个日期之外的键值都进行ip删除
							ipset=hashmap.get(key);
							tsit=ipset.iterator();
							while(tsit.hasNext())
							{
								ip=tsit.next().toString();
								redisserver.del("src_"+key);
								redisserver.del("src_"+key+"_"+ip);
								redisserver.del("src_"+key+"_detail1_"+ip);
								redisserver.del("src_"+key+"_detail2_"+ip);
								test_ip_num+=1;
							}
						}
						hashmap.clear();
					}
	
					tsit = dstkeys.iterator();
					while(tsit.hasNext())
					{
						key=tsit.next().toString();
						ip=StringUtils.substringAfterLast(key, "_");	//获取key中的ip信息
						dt=redisserver.get(key);
						if(hashmap.containsKey(dt)==false)
						{
							ipset=new TreeSet<String>();
						}
						else{
							ipset=hashmap.get(dt);
						}
						ipset.add(ip);
						hashmap.put(dt, ipset);
					}
	
					if(hashmap.isEmpty()==false){
						ipset=new TreeSet<String>();
						ipset.addAll(hashmap.keySet());
						size=ipset.size()-1;
						hsit=ipset.iterator();
	
						for(int i=0;i<size;i++)
						{
							key=hsit.next().toString();//按照日期，除了最后一个日期之外的键值都进行ip删除
							ipset=hashmap.get(key);
							tsit=ipset.iterator();
							while(tsit.hasNext())
							{
								ip=tsit.next().toString();
								redisserver.del("dst_"+key);
								redisserver.del("dst_"+key+"_"+ip);
								redisserver.del("dst_"+key+"_detail1_"+ip);
								redisserver.del("dst_"+key+"_detail2_"+ip);
								test_ip_num+=1;
							}
						}
						hashmap.clear();
					}

					//释放内存
					redisserver=null;
					srckeys=null;
					dstkeys=null;
					tsit=null;
					hsit=null;
					key=null;
					dt=null;
					ip=null;
					hashmap=null;
					ipset=null;
					
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
