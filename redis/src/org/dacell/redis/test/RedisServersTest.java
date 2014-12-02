package org.dacell.redis.test;

import java.util.UUID;

import org.dacell.redis.cfg.RpcXmlConfigure;
import org.dacell.redis.servicemanager.RedisManager;
import org.dacell.redis.servicemanager.RedisServiceHandler;

public class RedisServersTest {
	public static RedisServiceHandler redisServiceHandler = null;
	//1.涓�洿浠庡悓涓�釜缂撳瓨鑾峰彇鏁版嵁锛屽簲璇ラ渶瑕侀殢鏈鸿幏鍙�
	//2.鑾峰彇璇婚樀鍒楃殑鏃跺�濡傛灉閲岄潰鐨刾ool宸茬粡姝讳骸锛屾病娉曟彁鍓嶉娴嬶紝鍙兘瀵艰嚧璇诲叏閮ㄦ浜＄殑鏃跺�鑾峰彇涓嶅埌鍐欑殑闃靛垪
	public static void main(String[] args){

//		Redis_KeySet_defDomain keySet=Redis_KeySet_defDomain.getInstance();
		//server
//		Redis_Server_defDomain server=new Redis_Server_defDomain();
//		server.setMax_active(500);
//		server.setMax_idle(1000 * 60);
//		server.setMax_wait(1000 * 10L);
//		
//		Redis_Server_instanceDomain instance1=new Redis_Server_instanceDomain();
//		instance1.setName("server1");
//		instance1.setHost("127.0.0.1");
//		instance1.setPort(6379L);
//		instance1.setRedis_Server_instanceDomain(server);
//		
//		Redis_KeyPattern_defDomain keypattern1=new Redis_KeyPattern_defDomain();
//		keypattern1.setPattern("^1.*");
//		keypattern1.addRedisServer(instance1);
//		keySet.addKeyPattern(keypattern1);
//		
//		Redis_Server_instanceDomain instance2=new Redis_Server_instanceDomain();
//		instance2.setName("server2");
//		instance2.setHost("127.0.0.1");
//		instance2.setPort(63791L);
//		instance2.setRead_only(false);
//		instance2.setRedis_Server_instanceDomain(server);
////		Redis_KeyPattern_defDomain keypattern2=new Redis_KeyPattern_defDomain();
////		keypattern2.setPattern("^2.*");
////		keypattern1.addRedisServer(instance2);
////		keySet.addKeyPattern(keypattern2);
//		
//		Redis_Server_instanceDomain instance3=new Redis_Server_instanceDomain();
//		instance3.setName("server3");
//		instance3.setHost("127.0.0.1");
//		instance3.setPort(63792L);
//		instance3.setRead_only(true);
//		instance3.setRedis_Server_instanceDomain(server);
////		Redis_KeyPattern_defDomain keypattern3=new Redis_KeyPattern_defDomain();
////		keypattern3.setPattern("^3.*");
//		keypattern1.addRedisServer(instance3);
////		keySet.addKeyPattern(keypattern3);
//		
//		Redis_Server_instanceDomain instance4=new Redis_Server_instanceDomain();
//		instance4.setName("server4");
//		instance4.setHost("127.0.0.1");
//		instance4.setPort(63793L);
//		instance4.setRead_only(true);
//		instance4.setRedis_Server_instanceDomain(server);
////		Redis_KeyPattern_defDomain keypattern4=new Redis_KeyPattern_defDomain();
////		keypattern4.setPattern("^4.*");
//		keypattern1.addRedisServer(instance4);
////		keySet.addKeyPattern(keypattern4);

		
//		System.out.println("server list size="+keypattern1.getRedisServerList().size());
//		redisManager.addCache2Pool(keySet);
//		RedisManager redisManager=RedisManager.getInstance();
//		redisManager.addCache2Pool(RpcXmlConfigure.getRedis_KeySet());
		//
		
		redisServiceHandler =RedisServiceHandler.getInstance();
//		redisServiceHandler.flushAll();
//		
//		for(int i=1;i<=300;i++){
//			testRedis();
//			try {
//				Thread.sleep(2*1000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		redisServiceHandler.set("test", "123");
		System.out.println(redisServiceHandler.get("test"));
		System.out.println(redisServiceHandler.get("test1"));
		System.out.println("it is over........");
//		for(long i=1;i<=100;i++){
//			redisServiceHandler.set(i+"", i);
//			System.out.println("hashcode:"+(i+"").hashCode()+"-----------"+redisServiceHandler.get(i+""));
//		}
//		for(long i=1;i<=100;i++){
//			System.out.println("hashcode:"+(i+"").hashCode()+"-----------"+redisServiceHandler.get(i+""));
//		}
//		for(long i=1;i<=100;i++){
//			redisServiceHandler.set(i+"", i);
//		}
//		redisServiceHandler.set("129074659", "6666677");
//		System.out.println("==="+redisServiceHandler.get("129074659"));
//		String str=JSONSerialize.serialize(instance3);
//		System.out.println("JSONSerialize="+str);
//		Redis_Server_instanceDomain tt=(Redis_Server_instanceDomain)(JSONSerialize.unSerialize(str));
//		System.out.println("*********************==="+tt.getHost());
		
//		redisServiceHandler.set("1453", "1111114");
//		redisServiceHandler.set("12453", "444444");
//		redisServiceHandler.set("13453", "555555");
//		redisServiceHandler.set("132453", "666666");
////		redisServiceHandler.set("2454", "1111115");
//		redisServiceHandler.set("3455", "1111116");
//		redisServiceHandler.set("31455", "333333");
//		redisServiceHandler.set("32455", "222222");
//		redisServiceHandler.set("313453", "7777777");
		
//		redisServiceHandler.get("13453");
//		redisServiceHandler.sAdd("19999", instance3);
//		redisServiceHandler.sAdd("19999", instance2);
//		redisServiceHandler.sAdd("19999", instance1);
		
//		redisServiceHandler.hset("1", "name", "guoming");
//		System.out.println(redisServiceHandler.hget("1", "name"));
//		Set instancedomian=(HashSet)redisManager.sMembers("19999");
//		Iterator<Redis_Server_instanceDomain> it=instancedomian.iterator();
//		while(it.hasNext()){
//		System.out.println("&&&&&&&&&&&&=="+((Redis_Server_instanceDomain)it.next()).getName());
//		}
	}
	public static void testRedis(){
		for(int i=1;i<=50;i++){
			RedisTask task = new RedisTask(i);
			task.start();
		}
	}
	static class RedisTask extends Thread {
		int j;
		
		public RedisTask(int j) {
			this.j = j;
		}

		public void run() {
			long start_time = System.currentTimeMillis();
			for(long i=j*100+1;i<=j*100+100;i++){
				String key = UUID.randomUUID().toString();
				redisServiceHandler.set(key, key);
				Object value = redisServiceHandler.get(key);
//				System.out.println("thread:"+j+";value:"+value);
				if(value==null){
					throw new RuntimeException("ddddddddddd");
				}
			}
			long end_time = System.currentTimeMillis();
			long time = end_time - start_time;
			System.out.println("线程"+j+"耗时:"+time+"(ms)");
        }
    }

}