package org.dacell.redis.redishandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dacell.redis.servicemanager.RedisPool;

public class SortedSetHandler {
	private RedisPool redisPool;
	private Log LOG = LogFactory.getLog(SortedSetHandler.class);

	public SortedSetHandler(RedisPool redisPool) {
		this.redisPool = redisPool;
	}

}
