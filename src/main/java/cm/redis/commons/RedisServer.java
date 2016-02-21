package cm.redis.commons;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.SortingParams;

/**
 * 2016-01-25 Jedis是2.8.0版本，对应的Redis服务器是3.0.6版本，用于构建cluster连接池与操作的封装类
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
		//Jedis Cluster will attempt to discover cluster nodes automatically
		jedisClusterNodes.add(new HostAndPort(ResourcesConfig.REDIS_SERVER_IP, 
				Integer.valueOf(ResourcesConfig.REDIS_SERVER_PORT)));
		
		//构建Cluster的连接池配置参数
		JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(ResourcesConfig.MAX_ACTIVE);
        config.setMaxIdle(ResourcesConfig.MAX_IDLE);
        config.setMaxWaitMillis(ResourcesConfig.MAX_WAIT);
        config.setTestOnBorrow(ResourcesConfig.TEST_ON_BORROW);
        //新建JedisCluster连接
        jedisCluster=new JedisCluster(jedisClusterNodes,
        		ResourcesConfig.CLUSTER_TIMEOUT,
        		ResourcesConfig.CLUSTER_MAX_REDIRECTIONS, 
        		config);
        
        //获取所有集群子节点
        clusterNodes=jedisCluster.getClusterNodes();
	}
	
	/**
	 * 获取缓存管理器唯一实例
	 * @return
	 */
	public static RedisServer getInstance() {
		return INSTANCE;
	}
	
	//关闭会话
	public void close(){
		 try {
			jedisCluster.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
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
		exists=jedisCluster.exists(key);
		return exists;
	}
	
	/**
	 * 删除key值
	 * @param key
	 * @return 被删除的键的数目
	 */
	public long del(String key){
		return jedisCluster.del(key);
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
	/*通用key操作结束*/
	
	/*String操作*/
	/**
	 * 添加string value值
	 * @param key
	 * @param value
	 */
	public void set(String key, String value){
		jedisCluster.set(key, value);
	}
	
	/**
	 * 返回string value值
	 * @param key
	 * @return
	 */
	public String get(String key){
		return jedisCluster.get(key);
	}
	
	/**
	 * 对键值进行自增计数，将指定主键key的value值加1，返回新值，key不存在则添加，value设为1
	 * @param key
	 * @return 返回最新的自增值
	 */
	public long incr(String key){
		return jedisCluster.incr(key);
	}
	/*String操作结束*/
	
	/*list操作封装*/
	/**
	 * 从list左边插入数值，如果key值不存在，会自动创建并添加元素
	 * @param key
	 * @param value
	 */
	public void lpush(String key,String value){
		jedisCluster.lpush(key, value);
	}
	
	/**
	 * 从list右边插入数据，如果key值不存在，会自动创建并添加元素
	 * @param key
	 * @param value
	 */
	public void rpush(String key,String value){
		jedisCluster.rpush(key, value);
	}
	
	/**
	 * 从list左边弹出值
	 * @param key
	 * @param value
	 * @return 弹出的值，key不存在返回null
	 */
	public String lpop(String key){
		return jedisCluster.lpop(key);
	}
	
	/**
	 * 从list从右边弹出值
	 * @param key
	 * @param value
	 * @return 弹出的值，key不存在返回null
	 */
	public String rpop(String key){
		return jedisCluster.rpop(key);
	}
	
	/**
	 * 返回获得的区域value list
	 * @param key
	 * @param start 起始位置，从0开始
	 * @param end -1代表数组最末位置，否则表示结束位置，从0开始计算
	 * @return
	 */
	public List<String> lrange(String key,long start, long end){ 
		return jedisCluster.lrange(key, start, end);
	}
	
	/*list操作封装结束*/
	
	/*set集合操作封装*/
	/**
	 * 添加set元素操作
	 * @param key
	 * @param value
	 */
	public void sadd(String key, String value){
		jedisCluster.sadd(key,value);
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
		return jedisCluster.hexists(key, field);
	}
	
	/**
	 * 将哈希表key中的域field的值设为value，key不存在，
	 * 一个新的哈希表被创建，域field已经存在于哈希表中，旧值将被覆盖
	 * @param key
	 * @param field
	 * @param value
	 */
	public void hset(String key, String field, String value)
	{
		jedisCluster.hset(key, field, value);
	}
	
	/**
	 * 设置哈希表中的字段及对应的值
	 * @param key
	 * @param field_value,哈希键值数组
	 */
	public void hmset(String key, Map<String, String> field_value)
	{
		jedisCluster.hmset(key, field_value);
	}
	
	/**
	 * 获取哈希表中域的值
	 * @param key
	 * @param field
	 * @return 如果key或者field不存在，结果返回为null
	 */
	public String hget(String key, String field)
	{
		return jedisCluster.hget(key, field);
	}
	
	/**
	 * 获取hash key对应的所有feild和value对
	 * @param key
	 * @return
	 */
	public Map<String, String> hgetall(String key)
	{
		return jedisCluster.hgetAll(key);
	}
	
	/**
	 * 获取hash中key下对应的所有域fields
	 * @param key
	 * @return
	 */
	public Set<String> hkeys(String key)
	{
		return jedisCluster.hkeys(key);
	}
	
	/**
	 * 删除hash中key对应的field
	 * @param key
	 * @param field
	 */
	public void hdel(String key, String field)
	{
		jedisCluster.hdel(key, field);
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
		return jedisCluster.sort(key);
	}
	
	/**
	 * 对redis中的list，set，order set对应的key值进行排序
	 * @param key
	 * @param sortingParameters
	 * @return list, set, order set中的排序结果
	 */
	public List<String> redis_sort2(String key, SortingParams sortingParameters)
	{
		return jedisCluster.sort(key, sortingParameters);
	}
	/*排序操作封装结束*/
	
	/*redis cluster不支持事务操作*/
}

