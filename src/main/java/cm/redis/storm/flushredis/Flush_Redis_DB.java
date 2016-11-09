package cm.redis.storm.flushredis;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import cm.redis.commons.RedisServer;
import cm.redis.commons.TimeFormatter;
import cm.sjsn.commons.G4jk_ref_Syn;

/**
 * 用于删除，更新redis中过期的数据
 * @author nicolashsu 2016-09-08
 *
 */
public class Flush_Redis_DB {
	/*
	 * 对redis中关于storm存放在redis上的数据进行统一的清理工作
	 */
	public static Logger logger=Logger.getLogger(Flush_Redis_DB.class);
	
	/**
	 * 主函数
	 * @param args
	 */
	public static void main(String[] args)
	{
		//测试代码段
//		Flush_Redis_DB.flush_g4jk();
//		Flush_Redis_DB.flush_g4jk_ref();
		
		//正式代码段
		boolean cleanonce=false;
		while(true)
		{
			//每天固定凌晨3点清理一次数据
			if(TimeFormatter.getHour().equals("03")==true||TimeFormatter.getHour().equals("14")==true)
			{
				if(cleanonce==false){
					// 每天凌晨 3 点与下午14点执行，负责清理大日志数据过期的实时信息，暂停
					//Flush_Redis_DB.flush_biglogs();
					// 每天凌晨 3 点与下午14点执行，负责清理网分数据过期的实时信息
					Flush_Redis_DB.flush_g4jk();
				    if(TimeFormatter.getHour().equals("03")==true){ //每天更新一次维表信息
						// 每天凌晨 3 点检查维表更新，更新添加维表信息，如果获取不到最新数据，维表信息在redis中可能为空
						Flush_Redis_DB.flush_g4jk_ref();//仅清理掉昨天过期的触点参数信息数据
						// 获取接口数据，更新ref维表信息，所有数据文件第一行为列名，用;隔开，第二行开始是数据记录，记录内数据之间同样用分号隔开
						Flush_Redis_DB.update_g4jk_ref(null, "tcsll");		//直接对已有的ref文件进行更新，这个维表不会经常更新，ref文件:tb_mofang_tcsll_ref.txt
						Flush_Redis_DB.update_g4jk_ref(null, "webtag"); //直接对已有的ref文件进行更新，这个维表不会经常更新，ref文件:tb_mofang_webtag_ref.txt
						Flush_Redis_DB.update_g4jk_ref("d243c012-5ef5-4537-ad75-21c4b90fe74f","custtag");			//用户标签维表，标记用户的对应人群属性，每天更新一次，d243c012-5ef5-4537-ad75-21c4b90fe74f
						Flush_Redis_DB.update_g4jk_ref("c1ed7776-a16b-4472-a1bd-954df3925466", "hotspot");		//tac ci与热点区域转换维表，这个维表不会经常更新，c1ed7776-a16b-4472-a1bd-954df3925466
						Flush_Redis_DB.update_g4jk_ref("0b67bada-c954-418d-aa25-347b5810c679", "imsiphnum");  //号码与imsi转换表，每天更新一次，0b67bada-c954-418d-aa25-347b5810c679
				    }
				    cleanonce=true;
				}
			}
			else	{
				cleanonce=false;
				//每隔半个小时读取最新的接口触点关键字配置信息，用于提供给storm实时分析的指标
				Flush_Redis_DB.setSjjsParamsToRedis();
			}
					
			try{					
				Thread.sleep(1000*60*60);//休息1小时
			}catch(Exception e){
				logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
			}
		}
	}
	
	/**
	 * 对4G网分数据进行定期删除
	 */
	public static void flush_g4jk()
	{
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String date1=null;
		String date2=null;
		String key=null;
		int num=0;
		
		//获取实例
		redisserver=RedisServer.getInstance();

		num=0;
		logger.info(" Start to get g4jk storm-redis-keys");
		
		date1=TimeFormatter.getTheDayBeforYestoday2(); //获取前天的日期，删除前天热点的信息
		date2=TimeFormatter.getDate2();    //获取当前日期
		//仅删除大数据魔方相关的过期数据
		try {
			logger.info(" Start to clear g4jk storm-redis-keys which are out of date!!!");
			keys=redisserver.scan("mfg4*");  	//获取前天的全部信息
			if(keys!=null&&keys.size()>0){
				keylist = keys.iterator();
				while(keylist.hasNext())
				{
					key=keylist.next().toString();
					if(StringUtils.contains(key, date1)==true){
						redisserver.del(key);
						num+=1;
					}else if(StringUtils.contains(key, date2)==true){ //保留昨天的热力图和热点区域，电商热搜词
						if(StringUtils.contains(key, "hmset")==false
						&&StringUtils.contains(key, "hspset")==false
						&&StringUtils.contains(key, "ebusiw")==false){
							redisserver.del(key);
							num+=1;
						}
					}
				}
			}
			
			//释放内存
			redisserver=null;
			date1=null;
			date2=null;
			keys=null;
			keylist=null;
			key=null;
			
			logger.info(" Complete clear g4jk redis-keys, removes "+num+" g4 records (out of date)");					
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
		}
	}
	
	/**
	 * 对4G网分数据维表进行删除，2016-10-14，仅对接口，imsi与号码转换信息进行更新
	 */
	public static void flush_g4jk_ref()
	{
		RedisServer redisserver=null;
		TreeSet<String> keys=null; 
		Iterator<String> keylist =null;
		String key=null;
		int num=0;
		
		//获取实例
		redisserver=RedisServer.getInstance();

		num=0;
		logger.info(" Start to get g4jk_ref storm-redis-keys");

		try {
			logger.info(" Start to clear g4jk_ref storm-redis-keys which are out of date!!!");
			//2016-10-14添加，如果需要自动化，再进行更新
//			keys=redisserver.scan("ref_imsiphn_*"); 		//获取每天需要更新的ref 相关的keys，imsi与号码翻译是必须要每天更新一次的
//			if(keys!=null&&keys.size()>0){
//				keylist = keys.iterator();
//				while(keylist.hasNext())
//				{
//					key=keylist.next().toString();
//					redisserver.del(key);
//					num+=1;
//				}
//			}
			
			//2016-10-14添加，如果需要自动化，再进行更新
			keys=redisserver.scan("ref_sjjsparams_*"); 		//获取每天需要更新的ref 相关的keys，接口获取的sjjs信息也需要每天删除一次更新，这么做可以清理掉旧的已经不使用的数据
			if(keys!=null&&keys.size()>0){
				keylist = keys.iterator();
				while(keylist.hasNext())
				{
					key=keylist.next().toString();
					redisserver.del(key);
					num+=1;
				}
			}
			
			//释放内存
			redisserver=null;
			keys=null;
			keylist=null;
			key=null;
			
			logger.info(" Complete clear g4jk_ref redis-keys, removes "+num+" ref records (out of date)");
		} catch (Exception e) {
			logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
		}
	}
	
	/**
	 * 更新4G网分维表相关信息方法
	 * @param sjsn_id 接口提供的id
	 * @param private_folder 存放数据的对应文件夹名称，由用户自定义 最后不需要添加"/"，必须指定
	 */
	public static void update_g4jk_ref(String sjsn_id, String private_folder)
	{
			G4jk_ref_Syn g4jk_ref_Syn=new G4jk_ref_Syn();
			g4jk_ref_Syn.ref_data_syn(sjsn_id, private_folder);
			g4jk_ref_Syn=null;
	}
	
	/**
	 * 获取接口配置好的触点相关的关键信息，录入到redis，提供给实时系统用于计算
	 */
	public static void setSjjsParamsToRedis(){
		String url="http://10.245.254.110:8080/etl_platform/rest/service.shtml";
		String json="{\"identify\": \"d15040f9-d5f2-4b56-bfae-e22df035ef65\", \"userName\": \"STORM\", \"password\": \"Srm_xxy_2016\", \"systemName\": \"STORM\", \"parameter\":{}}";
		String returnjson=null;
		G4jk_ref_Syn g4jk_ref_Syn=new G4jk_ref_Syn();
		returnjson=g4jk_ref_Syn.getSjsnInterfaceInfo(url,json);
		String record=null;
		int split=0;
		String key=null;
		String value=null;
		//获取实例
		RedisServer redisserver=RedisServer.getInstance();
		if(returnjson!=null&&returnjson.equals("")==false){
			JSONObject jsonObject = JSON.parseObject(returnjson);
			JSONArray jsonArray= jsonObject.getJSONArray("records");
			if(jsonArray!=null&&jsonArray.size()>0)
			{
				for(int i=0;i<jsonArray.size();i++)
				{
					record=jsonArray.getString(i);
					if(record.length()>0){
						split=record.indexOf(",");
						//SJJS093,上网行为类型:购物#论坛;上网搜索热词:家宽#宽带#极光#电信;
						key=record.substring(0, split).trim().toUpperCase();
						value=record.substring(split+1);
						if(key.contains("SJJS")==true&&value.length()>0){
							key="ref_sjjsparams_"+key;
							redisserver.set(key, value);
						}
					}
				}
			}
		}else{
			logger.info("setSjjsParamsToRedis opt error: sjsn doesn't return any params");
		}
		
		g4jk_ref_Syn=null;
	}
	
}

//高流量低余额接口信息
//String add_url = "http://10.245.254.110:8080/etl_platform/rest/service.shtml";
//String json_params = "{\"identify\": \"26c068d5-5cf5-4951-9df4-0e597c4f0bbb\", \"userName\":\"ST_BIGDATA\",\"password\":\"Gmcc_345\",\"systemName\":\"STORM\",\"parameter\":{\"tm_intrvl_cd\":\""+MyTime.getYestoday()+"\",\"flux\":\"100\",\"amt\":\"20\"}}";
//号码 IMSI IMEI 用户余额（元） DOU（MB） 4GDOU（MB） 流量剩余资源（MB） 4G剩余流量资源（MB）
//try{					
//logger.info(" Storm-redis-cleaner is sleeping...zzz...");
//Thread.sleep(1000*60*60);
//}catch(Exception e)
//{
//logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
//}

/**
 * 对大日志过期数据进行清理
 */
//public static void flush_biglogs()
//{
//	RedisServer redisserver=null;
//	TreeSet<String> keys=null; 
//	Iterator<String> keylist =null;
//	String date=null;
//	String key=null;
//	String dt=null;
//	int test_ip_num=0;
//	
//	//获取实例
//	redisserver=RedisServer.getInstance();
//
//	test_ip_num=0;
//	logger.info(" Start to get biglogs storm-redis-keys");
//	
//	date=TimeFormatter.getDate2(); //获取当前日期，需要确定本程序运行的系统环境时间是正确的时间
//	
//	//仅删除大日志相关的数据
//	try {
//		logger.info(" Start to clear biglogs storm-redis-keys which are out of date!!!");
//		keys=redisserver.scan("src*"); 		//获取所有的src keys
//		if(keys!=null&&keys.size()>0){
//			keylist = keys.iterator();
//			while(keylist.hasNext())
//			{
//				key=keylist.next().toString();
//				if(StringUtils.contains(key, "src_date")==false){
//					if(StringUtils.contains(key, date)==false){
//						redisserver.del(key);
//						test_ip_num+=1;
//					}
//				}
//				else{
//					dt=redisserver.get(key);
//					if(dt!=null)
//					{
//						if(StringUtils.equals(dt, date)==false){
//							redisserver.del(key);
//							test_ip_num+=1;
//						}
//					}
//				}
//			}				
//		}
//		
//		keys=redisserver.scan("dst*"); 		//获取所有的dst keys
//		if(keys!=null&&keys.size()>0){
//			keylist = keys.iterator();
//			while(keylist.hasNext())
//			{
//				key=keylist.next().toString();
//				if(StringUtils.contains(key, "dst_date")==false ){
//					if(StringUtils.contains(key, date)==false){
//						redisserver.del(key);
//						test_ip_num+=1;
//					}
//				}
//				else{
//					dt=redisserver.get(key);
//					if(dt!=null)
//					{
//						if(StringUtils.equals(dt, date)==false){
//							redisserver.del(key);
//							test_ip_num+=1;
//						}
//					}
//				}
//			}				
//		}
//		
//		test_ip_num=test_ip_num/5;
//		
//		//释放内存
//		redisserver=null;
//		date=null;
//		keys=null;
//		keylist=null;
//		key=null;
//		dt=null;
//		
//		logger.info(" Complete clear biglogs redis-keys, removes "+test_ip_num+" ip (out of date)");					
//	} catch (Exception e) {
//		logger.info(" Thread Flush_Redis_DB crashes: "+e.getMessage());
//	}
//}
