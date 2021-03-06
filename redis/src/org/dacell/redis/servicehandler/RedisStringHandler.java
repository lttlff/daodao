package org.dacell.redis.servicehandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.dacell.redis.domain.Redis_KeyServerIndexDomain;
import org.dacell.redis.servicemanager.KeyServerPools;
import org.dacell.redis.servicemanager.RedisManager;
import org.dacell.redis.servicemanager.RedisPool;
import org.dacell.redis.servicemanager.RedisServerPools;

public class RedisStringHandler {
	private RedisServerPools redisServerPools;
	private RedisManager redisManager;
	private static Log LOG = LogFactory.getLog(RedisStringHandler.class);

	public RedisStringHandler(RedisServerPools redisServerPools1,RedisManager redisManager1) {
		this.redisServerPools = redisServerPools1;
		this.redisManager=redisManager1;
	}
	public List<Object> mget(String... keys) {
		List<Object> result = new ArrayList<Object>();

		// 为keys分组，因为keys可能会被路由到不同的缓存服务器上去
		//每一组KeyServerPools可能会有多个key需要去查询
		//这里先不管读写，只需要查询出不同KEY都归属于什么KeyServerPools里面的
		Map<KeyServerPools, List<String>> map = new HashMap<KeyServerPools, List<String>>();
		for (String key : keys) {
//			KeyServerPools keyServerPools = pools.get(poolName).getKeyServerPools(key);
			KeyServerPools keyServerPools = redisServerPools.getKeyServerPools(key);
			if (map.get(keyServerPools) == null) {
				List<String> keyList = new ArrayList<String>();
				keyList.add(key);
				map.put(keyServerPools, keyList);
			} else {
				map.get(keyServerPools).add(key);
			}
		}

		for (Iterator<KeyServerPools> it = map.keySet().iterator(); it.hasNext();) {
			KeyServerPools keyServerPools = it.next();
			RedisPool redisPool = redisManager.getReadRedisPool(keyServerPools);//从每个keyServerPools里面获取到一个可以读取数据的redis
			try {
				Object[] paramKeys = map.get(keyServerPools).toArray();//获取需要查询几个长度的字符串
				String[] paramKeyStrs = new String[paramKeys.length];
				for(int i=0;i<paramKeys.length;i++){
					paramKeyStrs[i] = String.valueOf(paramKeys[i]);
				}
//				result.addAll(redisPool.mget(paramKeyStrs));//从reids获取数据并插入缓存
				result.addAll(redisPool.getStringHandler().mget(paramKeyStrs));//从reids获取数据并插入缓存
			} catch (Throwable e) {
				// errorStoreLog.log(poolName, ,"keys");
				// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
				LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
						+ redisPool.getRedis_Server_instanceDomain().getPort() + "] mget keys["
						+ map.get(keyServerPools).toString() + "] is error,the errormsg is:" + e.getMessage());
				redisManager.add2ExceptionPool(redisPool);//获取数据失败，插入异常列表
			}
		}

		return result;
	}
	
	public void set(String key, Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
//		System.out.println("writerPools="+writerPools.size());
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					System.out.println("redisPool="+redisPool);
//					redisPool.set(key, value);
					redisPool.getStringHandler().set(key, value);
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error("pool[" + redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] set key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
					throw new RuntimeException("pool[" + redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] set key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
				}
			}
		}
	}
	//批量插入key add by sunJiaming 20140417
	public void setKeys(Map<String,Object> map) {
		Set<Integer> serverIndexs = new HashSet<Integer>();//存放批量插入涉及到的serverpool索引
		Map<Integer,ArrayList<Redis_KeyServerIndexDomain>> serverKeys = new HashMap<Integer, ArrayList<Redis_KeyServerIndexDomain>>();//将批量的key根据serverpool索引分组
		List<Redis_KeyServerIndexDomain> ksiList = new ArrayList<Redis_KeyServerIndexDomain>();//某个key对应的serverpool索引
		for(Iterator<String> iter = map.keySet().iterator();iter.hasNext();){
			String key = iter.next();
			int index = redisServerPools.getKeyServerPoolsIndex(key);
			serverIndexs.add(index);
			Redis_KeyServerIndexDomain ksi = new Redis_KeyServerIndexDomain(index,key,map.get(key));
			ksiList.add(ksi);
		}
		for(Iterator<Integer> iter = serverIndexs.iterator();iter.hasNext();){
			ArrayList<Redis_KeyServerIndexDomain> keys = new ArrayList<Redis_KeyServerIndexDomain>();
			int index = iter.next();
			for(Redis_KeyServerIndexDomain domain : ksiList){
				if(domain.getServerIndex() == index){
					keys.add(domain);
				}
			}
			serverKeys.put(index, keys);
		}
		for(Iterator<Integer> iter = serverKeys.keySet().iterator();iter.hasNext();){
			int index = iter.next();
			List<RedisPool> writerPools = redisServerPools.getWriterPoolsByIndex(index);
			for (RedisPool redisPool : writerPools) {
				if (!redisPool.isDead()) {
					try {
						redisPool.getStringHandler().setKeys(serverKeys.get(index));
					} catch (Throwable e) {
						// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
						LOG.error("pool[" + redisPool.getRedis_Server_instanceDomain().getHost()
								+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] set keys is error,the errormsg is:" + e.getMessage());
						redisManager.add2ExceptionPool(redisPool);
					}
				}
			}
		}
		
	}
	public Long setnx(String key, Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
//		System.out.println("writerPools="+writerPools.size());
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					System.out.println("redisPool="+redisPool);
//					redisPool.set(key, value);
					return redisPool.getStringHandler().setnx(key, value);
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error("pool[" + redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] set key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
		return 0l;
	}
	public void append(String key, Object value) {
		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
//		System.out.println("writerPools="+writerPools.size());
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					System.out.println("redisPool="+redisPool);
//					redisPool.set(key, value);
					redisPool.getStringHandler().append(key, value);
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error("pool[" + redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] set key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	public void set_has_expire(String key, Object value, int expireTime) {

		List<RedisPool> writerPools = redisServerPools.getWriterPools(key);
		for (RedisPool redisPool : writerPools) {
			if (!redisPool.isDead()) {
				try {
//					redisPool.set(key, value, Long.valueOf(expireTime));
					redisPool.getStringHandler().set(key, value, Long.valueOf(expireTime));
				} catch (Throwable e) {
					// errorStoreLog.log(poolName, ,"keys");
					// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
					LOG.error(redisPool.getRedis_Server_instanceDomain().getHost()
							+ ":" + redisPool.getRedis_Server_instanceDomain().getPort() + "] set key[" + key
							+ "] is error,the errormsg is:" + e.getMessage());
					redisManager.add2ExceptionPool(redisPool);
				}
			}
		}
	}
	
	public Object get(String key) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't get the key[" + key + "]!");
			return null;
		}
		try {
//			return redisPool.get(key);
			return redisPool.getStringHandler().get(key);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] expire[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			throw new RuntimeException(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] expire[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
		}
	}
	
	public Object getSet(String key,Object value) {
		RedisPool redisPool = redisManager.getReadRedisPool(key);
		if (redisPool == null) {
			LOG.debug("] is dead,so can't get the key[" + key + "]!");
			return null;
		}
		try {
			return redisPool.getStringHandler().getSet(key, value);
		} catch (Throwable e) {
			// errorStoreLog.log(poolName, ,"keys");
			// "这里需要记录同步日志，这些数据是需要在服务器可用的使用同步再次插入"；
			LOG.error(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] expire[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
			redisManager.add2ExceptionPool(redisPool);
			throw new RuntimeException(redisPool.getRedis_Server_instanceDomain().getHost() + ":"
					+ redisPool.getRedis_Server_instanceDomain().getPort() + "] expire[" + key
					+ "] is error,the errormsg is:" + e.getMessage());
		}
	}
}
