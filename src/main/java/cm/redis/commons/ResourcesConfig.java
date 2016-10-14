package cm.redis.commons;

/**
 * 接口配置类，用于记录portal接口、短信接口的注册信息
 * @author nicolashsu
 *
 */
public class ResourcesConfig {
	// 系统标识、系统名称
	public final static String SYSTEM_ID="storm";     
	public final static String SYSTEM_NAME="汕头移动业支中心实时计算系统";    
	public final static String SYSTEM_COPYRIGHT="中国移动通信集团广东有限公司汕头分公司业务支持中心运营管理室";
	
	public final static int MAX_ACTIVE = -1;	 									//可用连接实例的最大数目，默认值为8；如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
	public final static int MAX_IDLE = 10;			 								//控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值是8。空闲代表可以复用。
	public final static int MIN_IDLE = 5;			 									//控制一个pool最少有多少个状态为idle(空闲的)的jedis实例，默认值是0。空闲代表可以复用。
	public final static int MAX_WAIT =-1;  											//等待可用连接的最大时间，单位毫秒，默认值为-1，表示等待永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
	public final static boolean TEST_ON_BORROW = true;					//在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	
	public final static int CLUSTER_TIMEOUT=60*1000;						//获取集群信息的超时时间
	public final static int CLUSTER_MAX_REDIRECTIONS=15; 			//获取重定向的次数
	
	public final static String REDIS_SERVER_IP="10.245.254.59";		//redis数据库IP地址
	public final static String REDIS_SERVER_PORT="7000";				//redis数据库端口
	
	// 数据枢纽接口的文件，以及数据存放路径，最后带有/
	public final static String SYN_SERVER_DATAFILE="/home/storm/netantdata/ref/";  //正式服务器地址
	//public final static String SYN_SERVER_DATAFILE="E:/";  					//测试地址
	
}
