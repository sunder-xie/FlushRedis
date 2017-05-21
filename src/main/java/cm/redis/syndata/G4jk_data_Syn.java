package cm.redis.syndata;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cm.redis.commons.FileServer;
import cm.redis.commons.RedisServer;
import cm.redis.commons.ResourcesConfig;
import cm.redis.commons.TimeFormatter;

import redis.clients.jedis.Tuple;



/**
 * 用于同步redis上的基础模型数据信息，作为后续模型处理的基础数据
 * @author chinamobile 20170519
 *
 */
public class G4jk_data_Syn {
	public static Logger logger=Logger.getLogger(G4jk_data_Syn.class);
	
	/**
	 * 获取 storm 每天可以满足触点需求的 号码量 根据流量使用情况放入到文件中，
	 * 文件在10.245.254.56上自动入库到hdfs中，用于后续场景分析挖掘，形成规则
	 */
	public void putUntouchSetPhnumsInfoToFile(){
		RedisServer redisServer=RedisServer.getInstance();
		FileServer fileserver=FileServer.getInstance();
		String tdate=TimeFormatter.getYestoday2();		//仅获取昨天的数据
		String key=null;
		TreeSet<String> RedisUnTouchPhones=null;
		Iterator<String> phnlist=null;
		String phnnum=null;
		Set<Tuple> fretuples=null;
		String appid=null;
		String appname=null;
		Set<String> timestuples=null;
		String timestamp=null;
		Set<String> placestuples=null;
		String placestamp=null;
		String lalg=null;
		String[] divide=null;
		String contentline=null;

		double fre=0.0;
		
		int total=0;
		int usercatch=0;
		
		//获取全量触点的号码
		logger.info(" Thread G4jk_data_Syn "+TimeFormatter.getNow() +" starts...");
		key="mfg4_"+tdate+"_UnTouchSet";
		RedisUnTouchPhones=redisServer.sscan(key, null);
		
		if(RedisUnTouchPhones!=null&&RedisUnTouchPhones.size()>0){
			total=0;
			usercatch=0;
			phnlist=RedisUnTouchPhones.iterator();
			contentline="";
			while(phnlist.hasNext()){
				//获取每天使用频次超过10次以上的app，类别归属视频，音乐，游戏，网络购物，影音图像
				phnnum=phnlist.next().toString();
				if(phnnum!=null){
					key="mfg4_"+tdate+"_AppPoint_fre_"+phnnum;
					fretuples=redisServer.zrevrangebyscorewithscores(key, "10", "+inf");
					key="mfg4_"+tdate+"_AppPoint_times_"+phnnum;
					timestuples=redisServer.zrevrangebyscore(key, "10", "+inf");
					key="mfg4_"+tdate+"_AppPoint_places_"+phnnum;
					placestuples=redisServer.zrevrangebyscore(key, "10", "+inf");
					if(fretuples!=null&&fretuples.size()>0){
						for(Tuple fretpl: fretuples){
							appid=fretpl.getElement();
							fre=fretpl.getScore();
							key="ref_wtag_"+appid;
							appname=redisServer.get(key);
							if(appname!=null&&
							(StringUtils.contains(appname, "视频")==true||StringUtils.contains(appname, "音乐")==true||
							StringUtils.contains(appname, "游戏")==true||StringUtils.contains(appname, "网络购物")==true||
							StringUtils.contains(appname, "影音图像")==true)){
								usercatch+=1;
								timestamp="";
								for(String timestpl:timestuples){
									divide=timestpl.split("_");
									if(divide!=null&&divide.length>=3)
									{
										if(StringUtils.equals(appid, divide[2])==true)
											timestamp+=(divide[0]+"_"+divide[1]+";");
									}
								}
								placestamp="";
								for(String placestpl:placestuples){
									divide=placestpl.split("_");
									if(divide!=null&&divide.length>=3)
									{
										if(StringUtils.equals(appid, divide[2])==true){
											key="ref_ll_"+divide[0]+"_"+divide[1];
											lalg=redisServer.get(key);
											placestamp+=(lalg+";");
										}
									}
								}
								
								//写入文件
								contentline+=phnnum+"|"+appid+"|"+appname+"|"+fre+"|"+timestamp+"|"+placestamp+"\n";
								if(usercatch%20==0){
									fileserver.setWordsToFile(contentline, ResourcesConfig.RECORD_DATAFILE);
									contentline="";
								}
							}
						}
					}
				}
				total+=1;
			}
		}
		
		logger.info(" Thread G4jk_data_Syn "+TimeFormatter.getNow() +" completes processing phones: "+total);
		logger.info(" Thread G4jk_data_Syn "+TimeFormatter.getNow() +" catches records: "+usercatch);
		//释放内存
		redisServer=null;
		fileserver=null;
		tdate=null;
		key=null;
		RedisUnTouchPhones=null;
		phnlist=null;
		phnnum=null;
		fretuples=null;
		appid=null;
		appname=null;
		timestuples=null;
		timestamp=null;
		divide=null;
		placestuples=null;
		placestamp=null;
		lalg=null;
		divide=null;
		contentline=null;
	}
}
