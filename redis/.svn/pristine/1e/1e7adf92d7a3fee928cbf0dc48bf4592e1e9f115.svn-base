package org.dacell.redis.servicehandler;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dacell.redis.servicemanager.RedisManager;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.servicemanager.RedisServerPools;

public class RedisListHandler {
	private RedisServerPools redisServerPools;
	private RedisManager redisManager;
	private static Log LOG = LogFactory.getLog(RedisListHandler.class);

	public RedisListHandler(RedisServerPools redisServerPools,RedisManager redisManager) {
		this.redisServerPools = redisServerPools;
		this.redisManager=redisManager;
	}
	
	public void lpush(String key, Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.lpush(key, value);
					redisPool.getListHandler().lpush(key, value);					
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] lpush key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	
	public void lpush_has_expire(String key, Object value, int expireTime) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.lpush(key, value, Long.valueOf(expireTime));
					redisPool.getListHandler().lpush(key, value, Long.valueOf(expireTime));
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] lpush key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	public void lset(String key, int index,Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.rpush(key, value);
					redisPool.getListHandler().lset(key, index, value);				
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] rpush key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	public void rpush(String key, Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.rpush(key, value);
					redisPool.getListHandler().rpush(key, value);					
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] rpush key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	
	public void rpush_has_expire(String key, Object value, int expireTime) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.rpush(key, value, Long.valueOf(expireTime));
					redisPool.getListHandler().rpush(key, value, Long.valueOf(expireTime));
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] rpush key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	
	public Object lpop(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't lpop the key[" + key + "]!");
			return null;
		}
		try {
//			return redisPool.lpop(key);
			return redisPool.getListHandler().lpop(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] lpush key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	
	public Object rpop(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't rpop the key[" + key + "]!");
		}
		try {
//			return redisPool.lpop(key);
			return redisPool.getListHandler().rpop(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] rpop key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	
	public Long lLength(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't rpop the key[" + key + "]!");
		}
		try {
//			return redisPool.lLength(key);
			return redisPool.getListHandler().lLength(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] lLength key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return -1l;
		}
	}
	
	public Long lRemove(String key, String value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		Long delCount = 0l;
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					Long result = redisPool.lRemove(key, value);
					Long result = redisPool.getListHandler().lRemove(key, value);
					if (result > 0) {
						delCount += result;
					}
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] lRemove key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
		return delCount;
	}
	public Object lindex(String key,int index) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't rpop the key[" + key + "]!");
		}
		try {
//			return redisPool.lpop(key);
			return redisPool.getListHandler().lindex(key, index);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] rpop key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	public List<Object> lRange(String key, int start, int end) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't lRange the key[" + key + "]!");
			return null;
		}
		try {
//			return redisPool.lRange(key, start, end);
			return redisPool.getListHandler().lRange(key, start, end);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] lRange key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
	public List<Object> lget(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't lRange the key[" + key + "]!");
			return null;
		}
		try {
//			return redisPool.lRange(key, start, end);
			return redisPool.getListHandler().lget(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] lRange key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}
//	@Override
	public boolean blpop(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't blpop the key[" + key + "]!");
			return false;
		}
		try {
//			return redisPool.blpop(key);
			return redisPool.getListHandler().blpop(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] blpop key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return false;
		}
	}

	public boolean brpop(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't brpop the key[" + key + "]!");
			return false;
		}
		try {
//			return redisPool.brpop(key);
			return redisPool.getListHandler().brpop(key);			
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] brpop key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return false;
		}
	}

}
