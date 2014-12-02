package org.dacell.redis.servicehandler;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dacell.redis.servicemanager.RedisManager;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.servicemanager.RedisServerPools;

public class RedisSetHandler {
	private RedisServerPools redisServerPools;
	private RedisManager redisManager;
	private static Log LOG = LogFactory.getLog(RedisSetHandler.class);

	public RedisSetHandler(RedisServerPools redisServerPools,RedisManager redisManager) {
		this.redisServerPools = redisServerPools;
		this.redisManager=redisManager;
	}
	
	
	
	public void sAdd(String key, Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.sAdd(key, value);
					redisPool.getSetHandler().sAdd(key, value);
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] sAdd key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}

	public void sAdd_has_expire(String key, Object value, int expireTime) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.sAdd(key, value, Long.valueOf(expireTime));
					redisPool.getSetHandler().sAdd(key, value, Long.valueOf(expireTime));
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] sAdd key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}

	public Object sPop(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);

		if (redisPool == null) {
			LOG.debug("] is dead,so can't sPop the key[" + key + "]!");
			return null;
		}

		try {
//			return redisPool.sPop(key);
			return redisPool.getSetHandler().sPop(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] sPop key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}

	public Set<Object> sMembers(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);

		if (redisPool == null) {
			LOG.debug("] is dead,so can't sPop the key[" + key + "]!");
			return null;
		}

		try {
//			return redisPool.sMembers(key);
			return redisPool.getSetHandler().sMembers(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] sMembers key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return null;
		}
	}

	public Long sRem(String key, Object... values) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);

		if (redisPool == null) {
			LOG.debug("] is dead,so can't sRem the key[" + key + "]!");
			return 0l;
		}

		try {
//			return redisPool.sRem(key, values);
			return redisPool.getSetHandler().sRem(key, values);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] sRem key[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return -1l;
		}
	}

}
