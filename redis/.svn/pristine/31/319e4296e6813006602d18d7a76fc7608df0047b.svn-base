package org.dacell.redis.servicehandler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dacell.redis.servicemanager.KeyServerPools;
import org.dacell.redis.servicemanager.RedisManager;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.servicemanager.RedisServerPools;

public class RedisKeyHandler {
	private RedisServerPools redisServerPools;
	private RedisManager redisManager;
	private static Log LOG = LogFactory.getLog(RedisKeyHandler.class);

	public RedisKeyHandler(RedisServerPools redisServerPools,RedisManager redisManager) {
		this.redisServerPools = redisServerPools;
		this.redisManager=redisManager;
	}
	/**
	 * 删除所有缓存
	 *@author lff 2013-5-30 下午03:49:35
	 */
	public void flushAll(){
		List<KeyServerPools> keyServerPools = redisServerPools.getKeyServerPoolsList();//获取所有的keyServerPools阵列
		for (KeyServerPools keyPools : keyServerPools) {//遍历阵列
			if (keyPools.getWriterPools() != null && keyPools.getWriterPools().size()>0) {//如果没有可读的，获取可写的redis组，获取一个加入list返回
				for (RedisPool pool : keyPools.getWriterPools()) {
					if (!pool.isDead()) {
						pool.getKeyHandler().flushAll();
					}
				}
			}else if (keyPools.getReadPools() != null && keyPools.getReadPools().size()>0) {
				for (RedisPool pool : keyPools.getReadPools()) {//获得每组keyServerPools中一个可读没死的reids插入list返回
					if (!pool.isDead()) {
						pool.getKeyHandler().flushAll();
					}
				}

			}
		}
	}
	/**
	 * 从所有可能的缓存服务器上去获取,每个阵列获取一个可以获取数据的redis实例
	 */
	public Set<String> keys(String keyPattern) {
		Set<String> result = new HashSet<String>();

		List<KeyServerPools> keyServerPools = redisServerPools.getKeyServerPoolsList();//获取所有的keyServerPools阵列
		List<RedisPool> readPools = new ArrayList<RedisPool>();
		for (KeyServerPools keyPools : keyServerPools) {//遍历阵列
			if (keyPools.getReadPools() != null && keyPools.getReadPools().size()>0) {
				for (RedisPool pool : keyPools.getReadPools()) {//获得每组keyServerPools中一个可读没死的reids插入list返回
					if (!pool.isDead()) {
						readPools.add(pool);
						break;
					}
				}

			} else if (keyPools.getWriterPools() != null && keyPools.getWriterPools().size()>0) {//如果没有可读的，获取可写的redis组，获取一个加入list返回
				for (RedisPool pool : keyPools.getWriterPools()) {
					if (!pool.isDead()) {
						readPools.add(pool);
						break;
					}
				}
			}
		}//以上readPools的list中包含了可以获取所有缓存数据的可用的一个redis服务,下面进行遍历
		for (RedisPool redisPool : readPools) {
			try {
//				result.addAll(redisPool.keys(keyPattern));
				Set<String> keySet = redisPool.getKeyHandler().keys(keyPattern);
				if(keySet!=null){
					result.addAll(keySet);
				}
			} catch (Throwable e) {
				// errorStoreLog.log(poolName, ,"keys");
				// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
				LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
						+ redisPool.getRedis_Server_instanceDomain().getPort() + "] keys keypattern[" + keyPattern
						+ "] is error,the errormsg is:" + e.getMessage());
				redisManager.add2ExceptionPool(redisPool);//如果发现获取异常，将该redis加入异常池
			}
		}
		return result;
	}
	public Boolean exists(String key){
		RedisPool redisPool = redisManager.getReadRedisPool(key);

		try {
			return redisPool.getKeyHandler().exists(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] expire ttl[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return false;
		}
	}
	public void expire(String key, int expireTime){
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.expire(key, Long.valueOf(expireTime));
					redisPool.getKeyHandler().expire(key, Long.valueOf(expireTime));
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] expire[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	public Long persist(String key){
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		Long count = 0l;
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
					count += redisPool.getKeyHandler().persist(key);
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] expire[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
					return -1l;
				}
			}
		}
		return count;
	}
	public Long size(String key){
		RedisPool redisPool = redisManager.getReadRedisPool(key);

		try {
			return redisPool.getKeyHandler().size(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] expire ttl[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return -1l;
		}
	}
	public Long ttl(String key) {

		RedisPool redisPool = redisManager.getReadRedisPool(key);

		try {
//			return redisPool.ttl(key);
			return redisPool.getKeyHandler().ttl(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] expire ttl[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			return -1l;
		}
	}
	
	public int del(String key) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		int delCount = 0;
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					int result = redisPool.del(key);
					int result = redisPool.getKeyHandler().del(key);
					if (result > 0) {
						delCount++;
					}
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] del[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
		return delCount;
	}
	/**
	 * 删除符合的所有key
	 *@author lff 2013-6-4 下午02:35:20  
	 * @param keyPattern
	 * @return
	 */
	public int deleteMatchKey(String keyPattern){
		Set<String> keySet = this.keys(keyPattern);
		int delCount = 0;
		if(keySet !=null && keySet.size()>0){
			for(String key : keySet){
				delCount += this.del(key);
			}
		}
		return delCount;
	}

}
