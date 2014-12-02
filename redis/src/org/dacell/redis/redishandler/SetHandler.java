package org.dacell.redis.redishandler;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import org.dacell.redis.domain.Redis_KeySetDomain;
import org.dacell.redis.servicemanager.MonitorExcepRedisPoolThread;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.util.SerializeUtil;

public class SetHandler {
	private RedisPool redisPool;
	private Log LOG = LogFactory.getLog(StringHandler.class);

	public SetHandler(RedisPool redisPool) {
		this.redisPool = redisPool;
	}
	
	public void sAdd(String key, Object value) {
		this.sAdd(key, value,MonitorExcepRedisPoolThread.getDEFAULT_EXPIRE_SECONDS());
	}

	public void sAdd(String key, Object value, Long expireTime) {
		Jedis jedis = redisPool.getResource();
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] storeValue = SerializeUtil.serialize(value);

			if (expireTime > 0) {
				Pipeline pl = jedis.pipelined();
				pl.sadd(byteKey, storeValue);
				pl.expire(byteKey, expireTime.intValue());
				pl.sync();
			} else {
				jedis.sadd(byteKey, storeValue);
			}
			redisPool.returnResource(jedis);
			return;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool.getExceptionMsg("operator[sAdd] jedis is error!"), e);
		}

	}

	public Object sPop(String key) {
		Jedis jedis = redisPool.getResource();
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] value = jedis.spop(byteKey);
			redisPool.returnResource(jedis);
			return SerializeUtil.unSerialize(value);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool.getExceptionMsg("operator[sPop] jedis is error!"), e);
			return null;
		}
	}

	public Set<Object> sMembers(String key) {
		Jedis jedis = redisPool.getResource();
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			Set<byte[]> set = jedis.smembers(byteKey);
			redisPool.returnResource(jedis);
			return SerializeUtil.unSerializeSet(set);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool.getExceptionMsg("operator[sMembers] jedis is error!"), e);
			return null;
		}
	}

	public Long sRem(String key, Object... values) {
		Jedis jedis = redisPool.getResource();
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[][] storevalues = SerializeUtil.serializes(values);
			Long sremCount = jedis.srem(byteKey, storevalues);
			redisPool.returnResource(jedis);
			return sremCount;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool.getExceptionMsg("operator[sRem] jedis is error!"), e);
			return null;
		}
	}

}
