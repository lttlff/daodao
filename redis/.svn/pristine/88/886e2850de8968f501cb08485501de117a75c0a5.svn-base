package org.dacell.redis.servicemanager;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.dacell.redis.domain.Redis_KeyPatternDomain;
import org.dacell.redis.domain.Redis_KeySetDomain;
import org.dacell.redis.domain.Redis_ServerDomain;
//1.如果做持久化，需要把持久化实例从可读阵列区分开来
//2.准备一些主键的细分规则规则，方便以后分布式分片的实现
//3.JAVA锁机制对象的原理
//4.监听器启动
//5.如果所有的写服务死亡应该获取一个读服务转变为写服务
//6.主从互备关系直接可以通过XML配置的属性实现
//7.如果写服务挂掉的话，恢复的时候如何让他重新获取缓存的数据
public class RedisManager{
	private static RedisManager redisManager = null;
	private static final RedisServerPools redisServerPools=new RedisServerPools();
	private static final Map<RedisPool, List<Long>> expectionPool = new IdentityHashMap<RedisPool, List<Long>>();
//这个变量应该移到监控线程中去
// private static ErrorStoreLog errorStoreLog = new DefaultErrorStoreLog();

	private static boolean isInit = false;
	private Thread monitorExcepRedisPoolThread;
	private static Lock lock = new ReentrantLock();
//	private static Log LOG = LogFactory.getLog(RedisManager.class);
	private RedisManager(){
	}
	
	public static RedisManager getInstance() {
		if (redisManager == null) {
			redisManager = new RedisManager();
		}
		return redisManager;
	}
	
	public void initPool(Redis_KeySetDomain redis_KeySet_defDomain) {
		// 这里应该用锁对象
		if (!isInit) {
			isInit = true;
			try {
				addCache2Pool(redis_KeySet_defDomain);
			} catch (RuntimeException e) {
				// 如果创建失败应该清空对象
				this.isInit = false;
			}
		}
	}

	//针对一种缓存服务器阵列的构建，这里是redis，下面initPool初始化的时候包含对所有服务器的构建
	public void addCache2Pool(Redis_KeySetDomain redis_KeySet_defDomain){
		if(redis_KeySet_defDomain == null){
			return;
		}
		//有多少规则就会有多少redis服务器，所以入口可以在规则这边
		List<Redis_KeyPatternDomain> keyPatternList = redis_KeySet_defDomain.getKeyPatternList();//获取KeySet完整的keypattern集合
		List<KeyServerPools> keyServerPoolsList = new ArrayList<KeyServerPools>();//里面包含各分组的KeyServerPools,所有Key规则对应的redis服务集群列表，被包含在redisServerPools中
//		RedisServerPools redisServerPools = new RedisServerPools();//里面包含keyServerPoolsList，KeySet规则全集的redis服务集群,并包含KeySet集合
		redisServerPools.setRedis_KeySet_defDomain(redis_KeySet_defDomain); //这句的用意模糊，貌似可以省去
		//遍历获取的KeySet规则全集列表,获取下面服务器配置属性列表
		for (Redis_KeyPatternDomain redis_KeyPattern_defDomain : keyPatternList) {
			//每个规则对应一组redis读写组群,组群对应多个服务器redis_Server_defDomain,为每个服务器创建客户端连接池
			List<Redis_ServerDomain> redis_ServerList = redis_KeyPattern_defDomain.getRedisServerList();//获取keypattern对应的读写服务器组
			//准备构建单个规则的读写群组
			KeyServerPools keyServerPools = new KeyServerPools();//根据keypattern的每组读写服务器组构建开始
			List<RedisPool> writerList = new ArrayList<RedisPool>();
			List<RedisPool> readList = new ArrayList<RedisPool>();
			for (Redis_ServerDomain redis_Server_instanceDomain : redis_ServerList) {//遍历服务器参数配置对象列表，对redis初始化设值开始
				RedisPool poolElement = new RedisPool();
				poolElement.setRedis_Server_instanceDomain(redis_Server_instanceDomain);
//				poolElement.setJedisPool(createJedisPool(redis_Server_instanceDomain));//======传入属性，配置构建redispools
				poolElement.createJedisPool(redis_Server_instanceDomain);
				poolElement.initRedisHandler();//初始化RedisPool中对应的redis API操作
				boolean flag=false;
				if(Boolean.valueOf(redis_Server_instanceDomain.getServerAttrMap().get("readOnly"))){
					flag=true;
				}
				if (flag) {//可能要改变这个属性的位置，设置只读，感觉这个值应该在服务instance里面设置
//				if (redis_Server_instanceDomain.isRead_only()) {//可能要改变这个属性的位置，设置只读，感觉这个值应该在服务instance里面设置
					readList.add(poolElement);//加入可读列表
					poolElement.setRead_only(true);
					
//					System.out.println("read===="+poolElement.getRedis_Server_instanceDomain().getName()+"readList="+readList.size());
				} else {
					writerList.add(poolElement);//加入可写列表
					poolElement.setRead_only(false);
//					System.out.println("write===="+poolElement.getRedis_Server_instanceDomain().getName()+"writerList="+writerList.size());
				}
				poolElement.setKeyServerPools(keyServerPools);//新增的
			}
			//单个规则的读写群组构建成功,将属性注入
			keyServerPools.setRedis_KeyPattern_defDomain(redis_KeyPattern_defDomain);
			keyServerPools.setReadPools(readList);
			keyServerPools.setWriterPools(writerList);
			//我觉得不需要直接可以从本类获取
			keyServerPoolsList.add(keyServerPools);
//			if(!this.testConnect()){
//				distoryPool();
////				LOG.error("add CacheName["+redis_KeySet_defDomain.getName()+"] to pool is error,clear the CacheName.");
//			}
		}
		//这段代码从上面移下来
		redisServerPools.setKeyServerPoolsList(keyServerPoolsList);//某一类型的缓存服务器(如redis)的某个规则的缓存组构建完成
		RedisServiceHandler.initRedisService(redisServerPools,RedisManager.getInstance()).initRedisServiceHandler();
		
		if (monitorExcepRedisPoolThread == null) {
			monitorExcepRedisPoolThread = new MonitorExcepRedisPoolThread();
			monitorExcepRedisPoolThread.start();
		}
	}
	
	// 获取阵列中其中一个可获取数据的可读redis实例
	public RedisPool getReadRedisPool(String key) {
		List<RedisPool> readPools = redisServerPools.getReaderPools(key);
		if (readPools == null || readPools.size() == 0) {
			return null;
		} else {
			for (RedisPool redisPool : readPools) {
				if (!redisPool.isDead()) {
					return redisPool;
				}
			}
			//在死亡线程已经剔除的情况下的算法，否则用上面的
//			Random random = new Random();
//			int i=(Math.abs(random.nextInt()))%readPools.size();
//			RedisPool temp=readPools.get(i);
//			if (!temp.isDead()){
//				return temp;
//			}
		}
		// 如果某个缓存对象下的所有连接池都死完了，就将该缓存对象设置为死亡状态
		return null;
	}

	// 查询出某个阵列中可获取数据的redis实例，如果没有可读的，获取一个可写redis实例
	public RedisPool getReadRedisPool(KeyServerPools keyServerPools) {
		if (keyServerPools == null) {
			return null;
		}
		List<RedisPool> readPools = keyServerPools.getReadPools();
		if (readPools == null || readPools.size() == 0) {
			readPools = keyServerPools.getWriterPools();
		}
		for (RedisPool redisPool : readPools) {
			if (!redisPool.isDead()) {
				return redisPool;
			}
		}
		return null;
	}

	public void add2ExceptionPool(RedisPool pool) {
		if (!expectionPool.containsKey(pool)) {
			expectionPool.put(pool, new ArrayList<Long>());
		}
		if (expectionPool.get(pool).size() < MonitorExcepRedisPoolThread.getEXCEPNUM_TO_DEATH()) {
			expectionPool.get(pool).add(System.currentTimeMillis());
		} else {
			pool.setDead(true);
		}
	}

	public void distoryPool() {
		lock.lock();
		try {
			List<RedisPool> redisPools = redisServerPools.getAllPools();

			for (RedisPool redisPool : redisPools) {
//				if (!redisPool.isDead()) {
				if(redisPool !=null){
					redisPool.distoryPool();
					expectionPool.remove(redisPool);
					redisPool = null;
				}
			}
//			CacheServerPools cacheServerPools = redisServerPools;
			//=============================下面这段不能注释需要完善=======================================
//			redisServerPools = null;
//			pools.remove(poolName);
		} finally {
			lock.unlock();
		}
	}

	public static Map<RedisPool, List<Long>> getExpectionPool() {
		return expectionPool;
	}
}
//5.27晚在RedisRools新增了2个属性,并对dead属性的接口进行了改写，并对两个属性在此类进行了初始化设置
//5.28晚实现了规则读取阵列的可读redis服务器随机读取，修改了循环遍历获取可读的redis为随机获取一个可读的