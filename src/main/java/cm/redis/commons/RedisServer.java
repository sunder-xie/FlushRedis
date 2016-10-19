package cm.redis.commons;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import cm.redis.commons.ResourcesConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
//import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;

/**
 * 2016-09-1 Jedis是2.9.0版本，对应的Redis服务器是3.2.3版本，用于构建cluster连接池与操作的封装类
 * @author nicolashsu
 *
 */
public class RedisServer {
	//构建集群连接对象实例
	private static JedisCluster jedisCluster;
	
	//获取集群子节点
	private static Map<String, JedisPool> clusterNodes;  
	
	//单例模式实现客户端管理类
	private static RedisServer INSTANCE=new RedisServer();

	public static Logger logger=Logger.getLogger(RedisServer.class);
	
	//初始化构造函数
	private RedisServer()
	{
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		//只需要配置集群中的一个结点，连接成功后，自动获取点集群中其他结点信息
		jedisClusterNodes.add(new HostAndPort(ResourcesConfig.REDIS_SERVER_IP, 
				Integer.valueOf(ResourcesConfig.REDIS_SERVER_PORT)));
		
		//构建Cluster的连接池配置参数
//		JedisPoolConfig config = new JedisPoolConfig();
//        config.setMaxTotal(ResourcesConfig.MAX_ACTIVE);
//        config.setMaxIdle(ResourcesConfig.MAX_IDLE);
//        config.setMinIdle(ResourcesConfig.MIN_IDLE);
//        config.setMaxWaitMillis(ResourcesConfig.MAX_WAIT);
//        config.setTestOnBorrow(ResourcesConfig.TEST_ON_BORROW);
        
        //新建JedisCluster连接
		jedisCluster=new JedisCluster(jedisClusterNodes,ResourcesConfig.CLUSTER_TIMEOUT,ResourcesConfig.CLUSTER_MAX_REDIRECTIONS);
//        jedisCluster=new JedisCluster(jedisClusterNodes,
//        		ResourcesConfig.CLUSTER_TIMEOUT,
//        		ResourcesConfig.CLUSTER_MAX_REDIRECTIONS, 
//        		config);
        
        //获取所有集群子节点
        clusterNodes=jedisCluster.getClusterNodes();
	}
	
	/**
	 * 获取缓存管理器唯一实例
	 * @return
	 */
	public static RedisServer getInstance() {
		if(INSTANCE==null){
			synchronized (RedisServer.class) {
				if(INSTANCE==null){
					INSTANCE=new RedisServer();
				}
			}
		}
		return INSTANCE;
	}
	
	//关闭会话，销毁jediscluster对象
	public static void close(){
		 try {
			 if(jedisCluster!=null)jedisCluster.close();
		} catch (Exception e) {
			logger.error("Close jediscluster error: ", e);  
		}
	}
	
	/*通用key操作*/
	/**
	 * 判断key值是否存在
	 * @param key
	 * @return true为存在，false为不存在
	 */
	public boolean exists(String key){
		boolean exists=false;
		try{
			exists=jedisCluster.exists(key);
		}catch (Exception e) {
			logger.error("Jediscluster opt exists error: ", e); 
			return false;
		}
		return exists;
	}
	
	/**
	 * 删除key值
	 * @param key
	 * @return 被删除的键的数目
	 */
	public long del(String key){
		long delnum=0;
		try{
			delnum= jedisCluster.del(key);
		}catch (Exception e) {
			logger.error("Jediscluster opt del error: ", e); 
			return -1;
		}
		return delnum;
	}
	
	/**
	 * 自定义模糊匹配获取所有的keys
	 * @param pattern
	 * @return TreeSet，这个结构有个好处是是已经排序，可以直接获取第一个元素，默认升序排序
	 */
	public TreeSet<String> keys(String pattern){
        TreeSet<String> keys = new TreeSet<String>();
        for(String k : clusterNodes.keySet()){  
            JedisPool jp = clusterNodes.get(k);  
            Jedis connection = jp.getResource();  
            try {  
                keys.addAll(connection.keys(pattern));
                logger.info(" Get keys from"+connection.getClient().getHost() +":"+connection.getClient().getPort());
            } catch(Exception e){  
                logger.info(" Getting keys error: ", e);  
            } finally{  
                logger.info(" "+connection.getClient().getHost() +":"+connection.getClient().getPort()+" Connection closed.");  
                connection.close();//用完一定要close这个链接！！！
            }  
        }  
        return keys;  
    }
	
	/**
	 * 游标方式获取对应的key，比keys操作更加节省资源，便于使用
	 * @param cursor 游标，初始为0，最后返回为0，则表示遍历完成
	 * @param params 游标对应的匹配参数包括count和match
	 * @return
	 */
	public TreeSet<String> scan(String pattern){
		TreeSet<String> keys = new TreeSet<String>();
		ScanResult<String> scankey=null;
		List<String> tmplist=null;
		String cursor=null;
		ScanParams params=new ScanParams();
		int i=0;
        for(String k : clusterNodes.keySet()){  
            JedisPool jp = clusterNodes.get(k);  
            Jedis connection = jp.getResource();  
            try {
            	cursor="0";
            	params.match(pattern);
            	params.count(50);
            	do{
            		scankey=connection.scan(cursor, params);
            		if(scankey!=null)
            		{
            			cursor=scankey.getStringCursor();
            			tmplist=scankey.getResult();
            			if(tmplist!=null&&tmplist.size()>0){
            				if(tmplist.get(0).contains("empty list or set")==false)
            				{
            					for(i=0;i<tmplist.size();i++)keys.add(tmplist.get(i));//将String元素逐个加入，默认重复不会加入
            				}
            			}
            		}
            	}while(cursor.equals("0")==false); 
                logger.info(" Get keys from"+connection.getClient().getHost() +":"+connection.getClient().getPort());
            } catch(Exception e){  
                logger.info(" Getting keys error: ", e);  
            } finally{  
                logger.info(" "+connection.getClient().getHost() +":"+connection.getClient().getPort()+" Connection closed.");  
                connection.close();//用完一定要close这个链接！！！
            }  
        }  
        return keys;  
	}
	/*通用key操作结束*/
	
	/*单值操作，可以是String，Float*/
	/**
	 * 添加value值
	 * @param key
	 * @param value
	 * @return 
	 */
	public String set(String key, String value){
		String setres=null;
		try{
			setres=jedisCluster.set(key, value);
		}catch(Exception e){
			logger.error("Jediscluster opt set error: ", e);
			return null;
		}
		return setres;
	}
	
	/**
	 * 返回value值
	 * @param key
	 * @return
	 */
	public String get(String key){
		String getstr=null;
		try{
			getstr= jedisCluster.get(key);
		}catch(Exception e){
			logger.error("Jediscluster opt get error: ", e);
			return null;
		}
		return getstr;
	}
	
	/**
	 * 对键值进行自增计数，将指定主键key的value值加1，返回新值，key不存在则添加，value设为1
	 * @param key
	 * @return 返回最新的自增值
	 */
	public long incr(String key){
		long incr1=0;
		try{
			incr1= jedisCluster.incr(key);
		}catch(Exception e){
			logger.error("Jediscluster opt incr error: ", e);
			return -1;
		}
		return incr1;
	}
	
	/**
	 * 对键值进行自增浮点数计数，将指定主键key的value值加上浮点数，如果key本身不存在，会新增0并加上value
	 * @param key
	 * @param 
	 * @return 返回最新的自增值
	 */
	public Double incrbyfloat(String key,double value){
		double incrdoub=0.0;
		try{
			incrdoub=jedisCluster.incrByFloat(key, value);
		}catch(Exception e){
			logger.error("Jediscluster opt incrbyfloat error: ", e);
			return -1.0;
		}
		return incrdoub;
	}
	/*String操作结束*/
	
	/*list操作封装*/
	/**
	 * 从list左边插入数值，如果key值不存在，会自动创建并添加元素
	 * @param key
	 * @param value
	 * @return 返回插入后的元素个数
	 */
	public long lpush(String key,String value){
		long listnum=0;
		try{
			listnum=jedisCluster.lpush(key, value);
		}catch(Exception e){
			logger.error("Jediscluster opt lpush error: ", e);
			return -1;
		}
		return listnum;
	}
	
	/**
	 * 从list右边插入数据，如果key值不存在，会自动创建并添加元素
	 * @param key
	 * @param value
	 * @return 返回插入后的元素个数
	 */
	public long rpush(String key,String value){
		long listnum=0;
		try{
			listnum=jedisCluster.rpush(key, value);
		}catch(Exception e){
			logger.error("Jediscluster opt rpush error: ", e);
			return -1;
		}
		return listnum;
	}
	
	/**
	 * 从list左边弹出值
	 * @param key
	 * @param value
	 * @return 弹出的值，key不存在返回null
	 */
	public String lpop(String key){
		String popelement=null;
		try{
			popelement= jedisCluster.lpop(key);
		}catch(Exception e){
			logger.error("Jediscluster opt lpop error: ", e);
			return null;
		}
		return popelement;	
	}
	
	/**
	 * 从list从右边弹出值
	 * @param key
	 * @param value
	 * @return 弹出的值，key不存在返回null
	 */
	public String rpop(String key){
		String popelement=null;
		try{
			popelement=  jedisCluster.rpop(key);
		}catch(Exception e){
			logger.error("Jediscluster opt rpop error: ", e);
			return null;
		}
		return popelement;
	}
	
	/**
	 * 返回获得的区域value list
	 * @param key
	 * @param start 起始位置，从0开始
	 * @param end -1代表数组最末位置，否则表示结束位置，从0开始计算
	 * @return 区间结果集合
	 */
	public List<String> lrange(String key,long start, long end){
		List<String> rangres=null;
		try{
			rangres=jedisCluster.lrange(key, start, end);
		}catch(Exception e){
			logger.error("Jediscluster opt lrange error: ", e);
			return null;
		}
		return rangres;
	}
	
	/*list操作封装结束*/
	
	/*set集合操作封装*/
	/**
	 * 添加set元素操作
	 * @param key
	 * @param value
	 * @return 1添加成功，0已经存在，-1存在错误
	 */
	public long sadd(String key, String value){
		long res=0;
		try{
			res=jedisCluster.sadd(key,value);
		}catch(Exception e){
			logger.error("Jediscluster opt sadd error: ", e);
			return -1;
		}
		return res;
	}
	
	/**
	 * 获取set元素数量
	 * @param key
	 * @return 元素数量 -1表示出错
	 */
	public long scard(String key){
		long res=0;
		try{
			res=jedisCluster.scard(key);
		}catch(Exception e){
			logger.error("Jediscluster opt scard error: ", e);
			return -1;
		}
		return res;
	}
	
	/**
	 * 获取set的全部元素值集合
	 * @param key set的key
	 * @return 返回Set<String>集合
	 */
	public Set<String> smembers(String key){
		Set<String> res=null;
		try{
			res=jedisCluster.smembers(key);
		}catch(Exception e){
			logger.error("Jediscluster opt smembers error: ", e);
			return null;
		}
		return res;
	}
	
	/**
	 * 游标方式获取key对应的所有集合成员，比smembers操作更加节省资源，便于使用
	 * @param cursor 游标，初始为0，最后返回为0，则表示遍历完成
	 * @param params 游标对应的匹配参数包括count和match
	 * @return
	 */
	public TreeSet<String> sscan(String key, String pattern){
		TreeSet<String> members = new TreeSet<String>();
		ScanResult<String> scankey=null;
		List<String> tmplist=null;
		String cursor=null;
		ScanParams params=new ScanParams();
		int i=0;
        try {
        	cursor="0";
        	if(pattern!=null)params.match(pattern);
        	params.count(50);
        	do{
        		scankey=jedisCluster.sscan(key, cursor, params);
        		if(scankey!=null)
        		{
        			cursor=scankey.getStringCursor();
        			tmplist=scankey.getResult();
        			if(tmplist!=null&&tmplist.size()>0){
        				if(tmplist.get(0).contains("empty list or set")==false)
        				{
        					for(i=0;i<tmplist.size();i++)members.add(tmplist.get(i));//将String元素逐个加入，默认重复不会加入
        				}
        			}
        		}
        	}while(cursor.equals("0")==false); 
        } catch(Exception e){  
            logger.info(" Getting keys error: ", e); 
            return null;
        } 
        return members;  
	}
	/*set集合操作封装结束*/
	
	/*hash散列操作封装*/
	/**
	 * 判断对应哈希key，field是否存在
	 * @param key
	 * @param field
	 * @return
	 */
	public boolean hexists(String key, String field)
	{
		boolean res=false;
		try{
			res=jedisCluster.hexists(key, field);
		}catch(Exception e){
			logger.error("Jediscluster opt hexists error: ", e);
			return false;
		}
		return res;
	}
	
	/**
	 * 将哈希表key中的域field的值设为value，key不存在，
	 * 一个新的哈希表被创建，域field已经存在于哈希表中，旧值将被覆盖
	 * @param key
	 * @param field
	 * @param value
	 */
	public long hset(String key, String field, String value)
	{
		long res=0;
		try{
			res=jedisCluster.hset(key, field, value);
		}catch(Exception e){
			logger.error("Jediscluster opt hset error: ", e);
			return -1;
		}
		return res;
	}
	
	/**
	 * 设置哈希表中的字段及对应的值
	 * @param key
	 * @param field_value,哈希键值数组
	 * @return res 返回结果OK或者意外情况Exception
	 */
	public String hmset(String key, Map<String, String> field_value)
	{
		String res = null;
		try{
			res = jedisCluster.hmset(key, field_value);
		}catch(Exception e){
			logger.error("Jediscluster opt hmset error: ", e);
			return null;
		}
		return res;
	}
	
	/**
	 * 获取哈希表中域的值
	 * @param key
	 * @param field
	 * @return 如果key或者field不存在，结果返回为nil字符串，出错返回null
	 */
	public String hget(String key, String field)
	{
		String res = null;
		try{
			res=jedisCluster.hget(key, field);
		}catch(Exception e){
			logger.error("Jediscluster opt hget error: ", e);
			return null;
		}
		return res;
	}
	
	/**
	 * 获取hash key对应的所有feild和value对
	 * @param key
	 * @return res 返回所有的内容
	 */
	public Map<String, String> hgetall(String key)
	{
		Map<String, String> res =null;
		try{
			res =jedisCluster.hgetAll(key);
		}catch(Exception e){
			logger.error("Jediscluster opt hgetall error: ", e);
			return null;
		}
		return res;
	}
	
	/**
	 * 获取hash中key下对应的所有域fields
	 * @param key
	 * @return res 所有fields
	 */
	public Set<String> hkeys(String key)
	{
		Set<String> res =null;
		try{
			res = jedisCluster.hkeys(key);
		}catch(Exception e){
			logger.error("Jediscluster opt hkeys error: ", e);
			return null;
		}
		return res;
	}
	
	/**
	 * 删除hash中key对应的field
	 * @param key
	 * @param field
	 * @return res 删除成功1，无删除0，出错-1
	 */
	public long hdel(String key, String field)
	{
		long res=0;
		try{
			res=jedisCluster.hdel(key, field);
		}catch(Exception e){
			logger.error("Jediscluster opt hdel error: ", e);
			return -1;
		}
		return res;
	}
	/*hash散列操作封装结束*/
	
	/*排序操作封装*/
	/**
	 * 对redis中的list，set，order set对应的key值进行默认升序排序
	 * @param key
	 * @return list, set, order set中的排序结果
	 */
	public List<String> redis_sort1(String key)
	{
		List<String> res=null;
		try{
			res= jedisCluster.sort(key);
		}catch(Exception e){
			logger.error("Jediscluster opt redis_sort1 error: ", e);
			return null;
		}
		return res;
	}
	
	/**
	 * 对redis中的list，set，order set对应的key值进行排序
	 * @param key
	 * @param sortingParameters
	 * @return list, set, order set中的排序结果
	 */
	public List<String> redis_sort2(String key, SortingParams sortingParameters)
	{
		List<String> res=null;
		try{
			res= jedisCluster.sort(key, sortingParameters);
		}catch(Exception e){
			logger.error("Jediscluster opt redis_sort1 error: ", e);
			return null;
		}
		return res;
	}
	/*排序操作封装结束*/
	
	/*redis cluster不支持事务操作*/
}

