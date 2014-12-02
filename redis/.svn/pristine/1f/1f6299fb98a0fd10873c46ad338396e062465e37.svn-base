package org.dacell.redis.redishandler;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dacell.redis.domain.Redis_KeySetDomain;
import org.dacell.redis.servicemanager.MonitorExcepRedisPoolThread;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.util.SerializeUtil;

import redis.clients.jedis.Jedis;

public class KeyHandler {
	private RedisPool redisPool;
	private static Log LOG = LogFactory.getLog(KeyHandler.class);

	public KeyHandler(RedisPool redisPool) {
		this.redisPool = redisPool;
	}

	public Set<String> keys(String keyPattern) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(keyPattern);
			Set<byte[]> set = jedis.keys(byteKey);
			Set<String> keys = SerializeUtil.unSerializeKeySet(set);
			redisPool.returnResource(jedis);
			return keys;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e.getMessage());
		}
	}
	/**
	 * 是否存在
	 *@author lff 2013-6-26 上午11:22:42  
	 * @param key
	 * @return
	 */
	public Boolean exists(String key) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			Boolean bl = jedis.exists(byteKey);
			redisPool.returnResource(jedis);
			return bl;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e.getMessage());
		}
	}
	
	public void expire(String key) {
		this.expire(key, MonitorExcepRedisPoolThread.getDEFAULT_EXPIRE_SECONDS());
	}

	public void expire(String key, Long expireTime) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			jedis.expire(byteKey, expireTime.intValue());
			redisPool.returnResource(jedis);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			// redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[expire] jedis is error!"), e);
		}
	}
	/**
	 * 移除给定key的生存时间
	 *@author lff 2013-6-26 上午11:23:37  
	 * @param key
	 * @return
	 */
	public Long persist(String key) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			Long time = jedis.persist(byteKey);
			redisPool.returnResource(jedis);
			return time;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e.getMessage());
		}
	}
	
	public Long size(String key) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] value = jedis.get(byteKey);
			long size=0l;
			try {
				size = SerializeUtil.size(value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			redisPool.returnResource(jedis);
			return size;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[ttl] jedis is error!"), e);
			return 0l;
		}
	}
	
	public Long ttl(String key) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			Long expireTime = jedis.ttl(byteKey);
			redisPool.returnResource(jedis);
			return expireTime;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[ttl] jedis is error!"), e);
			return -1l;
		}
	}

	public int del(String key) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			Long result = jedis.del(byteKey);
			redisPool.returnResource(jedis);
			return result.intValue();
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e.getMessage());
		}
	}
	/**
	 * 删除所有
	 *@author lff 2013-5-30 下午03:55:04
	 */
	public void flushAll(){
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			jedis.flushAll();
			redisPool.returnResource(jedis);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			throw new RuntimeException(e.getMessage());
		}
	}
}
