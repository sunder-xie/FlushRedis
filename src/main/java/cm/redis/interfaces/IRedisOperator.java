package cm.redis.interfaces;

import java.util.TreeSet;

/**
 * 用于封装jediscluster操作相关的接口
 * @author chinamobile
 *
 */
public interface IRedisOperator {
	
	/** 
     * 根据pattern 获取所有的keys 
     * @param pattern 
     * @return 
     */  
    TreeSet<String> keys(String pattern);

}
