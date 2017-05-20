package cm.redis.syndata;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import cm.redis.commons.RedisServer;
import cm.redis.commons.TimeFormatter;



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
	public static void putUntouchSetPhnumsInfoToFile(){
		RedisServer redisServer=RedisServer.getInstance();
		String tdate=TimeFormatter.getDate2();		//仅获取当天的数据
		String key=null;
		TreeSet<String> RedisUnTouchPhones=null;
		Iterator<String> phonenum=null;
		
		List<UserAppPreferInfo> userAppPreferInfoList=null;
		int total=0;
		
		logger.info(" Thread G4jk_data_Syn "+TimeFormatter.getNow() +" starts...");
		key="mfg4_"+tdate+"_UnTouchSet";
		RedisUnTouchPhones=redisServer.sscan(key, null);
		
		if(RedisUnTouchPhones!=null&&RedisUnTouchPhones.size()>0){
			total=0;
			phonenum=RedisUnTouchPhones.iterator();
			while(phonenum.hasNext()){
				//
				
			}
		}
		
		logger.info(" Thread G4jk_data_Syn "+TimeFormatter.getNow() +" completes processing records: "+total);
	}
}
