package org.dacell.redis.redishandler;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import org.dacell.redis.domain.Redis_KeySetDomain;
import org.dacell.redis.servicemanager.MonitorExcepRedisPoolThread;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.util.SerializeUtil;

public class ListHandler {
	private RedisPool redisPool;
	private Log LOG = LogFactory.getLog(ListHandler.class);

	public ListHandler(RedisPool redisPool) {
		this.redisPool = redisPool;
	}

	public void lpush(String key, Object value) {
		// lpush(key, value, this.getDefaultLiveTime());
		lpush(key, value, -1L);
	}

	public void lpush(String key, Object value, Long expireTime) {
		Jedis jedis = redisPool.getResource();
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] storeValue = SerializeUtil.serialize(value);
			Pipeline pl = jedis.pipelined();
			pl.lpush(byteKey, storeValue);
			if (expireTime > 0) {
				pl.expire(byteKey, expireTime.intValue());
			}
			// pl.exec();
			pl.sync();
			redisPool.returnResource(jedis);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[lpush] jedis is error!"), e);
		}
	}

	public void rpush(String key, Object value) {
		this.rpush(key, value,MonitorExcepRedisPoolThread.getDEFAULT_EXPIRE_SECONDS());
	}
	public void rpush(String key, Object value, Long expireTime) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}

		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] storeValue = SerializeUtil.serialize(value);
			Pipeline pl = jedis.pipelined();
			pl.rpush(byteKey, storeValue);
			if (expireTime > 0) {
				pl.expire(byteKey, expireTime.intValue());
			}
			// pl.exec();
			pl.sync();
			redisPool.returnResource(jedis);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[rpush] jedis is error!"), e);
		}
	}

	public Object lpop(String key) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] storeValue = jedis.lpop(byteKey);
			redisPool.returnResource(jedis);
			Object value = SerializeUtil.unSerialize(storeValue);
			return value;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[lpop] jedis is error!"), e);
			return null;
		}
	}

	public Object rpop(String key) {
		Jedis jedis = redisPool.getResource();
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] storeValue = jedis.rpop(byteKey);
			redisPool.returnResource(jedis);
			Object value = SerializeUtil.unSerialize(storeValue);
			return value;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[rpop] jedis is error!"), e);
			return null;
		}
	}

	public Long lLength(String key) {
		Jedis jedis = redisPool.getResource();
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			Long length = jedis.llen(byteKey);
			redisPool.returnResource(jedis);
			return length;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[lLength] jedis is error!"), e);
			return -1l;
		}
	}
	public void lset(String key,int index,Object value) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] storeValue = SerializeUtil.serialize(value);
			jedis.lset(byteKey, index, storeValue);
			redisPool.returnResource(jedis);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[rpush] jedis is error!"), e);
		}
	}
	public Long lRemove(String key, String value) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] storeValue = SerializeUtil.serialize(value);
			Long delCount = jedis.lrem(byteKey, 0, storeValue);
			// pl.exec();
			redisPool.returnResource(jedis);
			return delCount;
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[lRemove] jedis is error!"), e);
			return 0l;
		}

	}
	public Object lindex(String key,int index) {
		Jedis jedis = redisPool.getResource();
		Object value = null;
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}

		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			byte[] byteValue = jedis.lindex(byteKey, index);
			value =  SerializeUtil.unSerialize(byteValue);
			redisPool.returnResource(jedis);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[rpush] jedis is error!"), e);
		}
		return value;
	}
	public List<Object> lRange(String key, int start, int end) {
		Jedis jedis = redisPool.getResource();
		if (jedis == null) {
			throw new RuntimeException("获取Jedis连接失败!");
		}
		try {
			byte[] byteKey = SerializeUtil.serializeKey(key);
			List<byte[]> result = jedis.lrange(byteKey, start, end);
			redisPool.returnResource(jedis);
			return SerializeUtil.unSerializeList(result);
		} catch (Exception e) {
			redisPool.returnBrokenResource(jedis);
			LOG.error(redisPool
					.getExceptionMsg("operator[lRange] jedis is error!"), e);
			return null;
		}
	}
	public List<Object> lget(String key) {
		return this.lRange(key, 0, -1);
	}
	public boolean blpop(String key) {
		return false;
	}

	public boolean brpop(String key) {
		return false;
	}

}
