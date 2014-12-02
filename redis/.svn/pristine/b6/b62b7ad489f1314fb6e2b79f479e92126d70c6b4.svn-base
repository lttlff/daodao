package org.dacell.redis.servicemanager;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dacell.redis.domain.Redis_KeySetDomain;

public class MonitorExcepRedisPoolThread extends Thread {
	private final static Map<RedisPool, List<Long>> expectionPool=RedisManager.getExpectionPool();
	
	private static Long DEFAULT_EXPIRE_SECONDS=Long.valueOf((String)Redis_KeySetDomain.getServerExecuteParam("default_expire_seconds"));
	private static int TEST_CONNECTION_COUNT = Integer.valueOf((String)Redis_KeySetDomain.getServerExecuteParam("test_connection_count"));
	
	private final static Long EXCEPNUM_TO_DEATH=Long.valueOf((String)Redis_KeySetDomain.getServerExecuteParam("excepnum_to_death"));
	private final static int SCAN_INTERVAL_MISECOND = Integer.valueOf((String)Redis_KeySetDomain.getServerExecuteParam("scan_interval_misecond"));

	public void run() {
		while (true) {
			try {
				sleep(SCAN_INTERVAL_MISECOND);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			for (Iterator<RedisPool> iterator = expectionPool.keySet().iterator(); iterator.hasNext();){
				RedisPool redisPool = iterator.next();
//				if (expectionPool.get(redisPool).size() < EXCEPNUM_TO_DEATH) {
//					if (!redisPool.testConnection()) {
//						redisPool.setDead(true);
//					}else{
//						redisPool.setDead(false);
//						expectionPool.remove(redisPool);
//					}
//				} else {
//					if (!redisPool.isDead()){
//						redisPool.setDead(true);
//					}
//				}
				if (!redisPool.isDead()) {
					if (expectionPool.get(redisPool).size() >= EXCEPNUM_TO_DEATH) {
						if (!redisPool.testConnection()) {
							redisPool.setDead(true);
							// expectionPool.put(redisPool, new
							// ArrayList<Long>());
						} else {
							redisPool.setDead(false);
							expectionPool.remove(redisPool);
						}
					}
				} else {
					if (redisPool.testConnection()) {
						redisPool.setDead(false);
						expectionPool.remove(redisPool);
					}
				}
			}
		}
	}

	public static Long getDEFAULT_EXPIRE_SECONDS() {
		return DEFAULT_EXPIRE_SECONDS;
	}

	public static int getTEST_CONNECTION_COUNT() {
		return TEST_CONNECTION_COUNT;
	}

	public static Long getEXCEPNUM_TO_DEATH() {
		return EXCEPNUM_TO_DEATH;
	}

	public static int getSCAN_INTERVAL_MISECOND() {
		return SCAN_INTERVAL_MISECOND;
	}
}