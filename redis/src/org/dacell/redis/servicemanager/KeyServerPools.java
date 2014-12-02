package org.dacell.redis.servicemanager;

import java.util.List;

import org.dacell.redis.domain.Redis_KeyPatternDomain;

/**
 * 某个规则对象的读写阵列
 */
public class KeyServerPools {
	private Redis_KeyPatternDomain redis_KeyPattern_defDomain;
	private List<RedisPool> writerPools;
	private List<RedisPool> readPools;

	public Redis_KeyPatternDomain getRedis_KeyPattern_defDomain() {
		return redis_KeyPattern_defDomain;
	}

	public void setRedis_KeyPattern_defDomain(
			Redis_KeyPatternDomain redis_KeyPattern_defDomain) {
		this.redis_KeyPattern_defDomain = redis_KeyPattern_defDomain;
	}

	public List<RedisPool> getWriterPools() {
		return writerPools;
	}

	public void setWriterPools(List<RedisPool> writerPools) {
		this.writerPools = writerPools;
	}

	public List<RedisPool> getReadPools() {
		return readPools;
	}

	public void setReadPools(List<RedisPool> readPools) {
		this.readPools = readPools;
	}
}
