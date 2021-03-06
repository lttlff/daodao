package org.dacell.redis.servicehandler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dacell.redis.servicemanager.KeyServerPools;
import org.dacell.redis.servicemanager.RedisManager;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.servicemanager.RedisServerPools;

public class RedisHashHandler {
	private RedisServerPools redisServerPools;
	private RedisManager redisManager;
	private static Log LOG = LogFactory.getLog(RedisHashHandler.class);

	public RedisHashHandler(RedisServerPools redisServerPools,RedisManager redisManager) {
		this.redisServerPools = redisServerPools;
		this.redisManager=redisManager;
	}
	
	public void hset(String key, String field, Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.hset(key, field, value);
					redisPool.getHashHandler().hset(key, field, value);
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] hset[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	
	public Object hget(String key, String field) {

		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't hget the key[" + key + "]!");
			return null;
		}
		try {
//			return redisPool.hget(key, field);
			return redisPool.getHashHandler().hget(key, field);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] hget[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	
	public Set<Object> hkeys(String key) {

		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't hget the key[" + key + "]!");
			return null;
		}
		try {
//			return redisPool.hget(key, field);
			return redisPool.getHashHandler().hkeys(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] hget[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	
	public List<Object> hmget(String key, String... fields) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't hget the key[" + key + "]!");
			return null;
		}
		try {
//			return redisPool.hmget(key, fields);
			return redisPool.getHashHandler().hmget(key, fields);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] hmget[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	
	public Map<String, Object> hgetAll(String key) {

		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't hgetAll the key[" + key + "]!");
			return null;
		}

		try {
//			return redisPool.hgetAll(key);
			return redisPool.getHashHandler().hgetAll(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] hgetAll[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	
	public int hdel(String key, String field) {

		int delCount = 0;
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					int result = redisPool.hdel(key, field);
					int result = redisPool.getHashHandler().hdel(key, field);
					if (result > 0) {
						delCount++;
					}
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] hdel[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
		return delCount;
	}
	
	/**
	 * 向一个value类型为hash数据的结构里面插入field-value,带过期时间
	 * 
	 * @param poolName
	 * @param key
	 * @param field
	 * @param value
	 * @param expireTime
	 */
	public void hset_has_expire(String key, String field, Object value, int expireTime) {

		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);

		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.hset(key, field, value);
					redisPool.getHashHandler().hset(key, field, value);
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] hset key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	
	/**
	 * 插入多个key-value键值队，并且value类型为hash数据的结构里面插入field-value
	 * 
	 * @param poolName
	 * @param keyfieldvalues
	 */
	public void mhmset(final Map<String, Map<String, Object>> keyfieldvalues) {

		// 为keys分组，因为keys可能会被路由到不同的缓存服务器上去
		Map<KeyServerPools, Map<String, Map<String, Object>>> routeMap = new HashMap<KeyServerPools, Map<String, Map<String, Object>>>();
		for (Iterator<String> it = keyfieldvalues.keySet().iterator(); it.hasNext();) {
			String key = it.next();
			Map<String, Object> fieldvalues = keyfieldvalues.get(key);

			KeyServerPools keyServerPools = redisServerPools.getKeyServerPools(key);
			if (routeMap.get(keyServerPools) == null) {
				Map<String, Map<String, Object>> map = new HashMap<String, Map<String, Object>>();
				map.put(key, fieldvalues);
				routeMap.put(keyServerPools, map);
			} else {
				routeMap.get(keyServerPools).get(key).putAll(fieldvalues);
			}
		}

		for (Iterator<KeyServerPools> it = routeMap.keySet().iterator(); it.hasNext();) {
			KeyServerPools keyServerPools = it.next();
			if (keyServerPools.getWriterPools() == null) {
				continue;
			}
			for (RedisPool redisPool : keyServerPools.getWriterPools()) {
				try {
//					redisPool.mhmset(routeMap.get(it));
					redisPool.getHashHandler().mhmset(routeMap.get(it));
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] mhmset keyfieldvalue["
							+ routeMap.get(it).toString() + "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	
	/**
	 * 插入多个key-value键值队，并且value类型为hash数据的结构里面插入field-value,带过期时间
	 * 
	 * keyExpire键值对需要注意： 1、如果key包含的expire为null，则按poolName配置的生存时间设置生存期
	 * 2、如果key包含的expire为0或者小于0，则表示永不过期 3、如果key包含的expire大于0，则按expire设置过期时间.
	 * 
	 * @param poolName
	 * @param keyfieldvalues
	 * @param keyexpires
	 */
	public void mehmset(final Map<String, Map<String, Object>> keyfieldvalues,
			final Map<String, Integer> keyexpires) {
		if (keyfieldvalues == null || keyexpires == null || keyfieldvalues.isEmpty())
			return;

		Map<KeyServerPools, Map<String, Map<String, Object>>> routeMap = new HashMap<KeyServerPools, Map<String, Map<String, Object>>>();
		Map<KeyServerPools, Map<String, Integer>> routeExpireMap = new HashMap<KeyServerPools, Map<String, Integer>>();
		for (Iterator<String> it = keyfieldvalues.keySet().iterator(); it.hasNext();) {
			String key = it.next();
			Map<String, Object> fieldvalues = keyfieldvalues.get(key);

			KeyServerPools keyServerPools = redisServerPools.getKeyServerPools(key);
			if (routeMap.get(keyServerPools) == null) {
				Map<String, Map<String, Object>> map = new HashMap<String, Map<String, Object>>();
				map.put(key, fieldvalues);
				routeMap.put(keyServerPools, map);
			} else {
				routeMap.get(keyServerPools).get(key).putAll(fieldvalues);
			}
		}

		for (Iterator<String> it = keyexpires.keySet().iterator(); it.hasNext();) {
			String key = it.next();
			Integer expireTime = keyexpires.get(key);
			KeyServerPools keyServerPools = redisServerPools.getKeyServerPools(key);
			if (routeExpireMap.get(keyServerPools) == null) {
				Map<String, Integer> map = new HashMap<String, Integer>();
				map.put(key, expireTime);
				routeExpireMap.put(keyServerPools, map);
			} else {
				routeExpireMap.get(keyServerPools).put(key, expireTime);
			}
		}

		for (Iterator<KeyServerPools> it = routeMap.keySet().iterator(); it.hasNext();) {
			KeyServerPools keyServerPools = it.next();
			if (keyServerPools.getWriterPools() == null) {
				continue;
			}
			for (RedisPool redisPool : keyServerPools.getWriterPools()) {
				try {
					if (routeExpireMap.get(keyServerPools) == null) {
//						redisPool.mhmset(routeMap.get(it));
						redisPool.getHashHandler().mhmset(routeMap.get(it));
					} else {
//						redisPool.mehmset(routeMap.get(it), routeExpireMap.get(it));
						redisPool.getHashHandler().mehmset(routeMap.get(it), routeExpireMap.get(it));
					}
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] mhmset keyfieldvalue["
							+ routeMap.get(it).toString() + "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}

}
