package org.dacell.redis.servicemanager;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.dacell.redis.domain.Redis_KeySetDomain;


/**
 * 包含规则集合和各规则对应的读写阵列* 
 */
public class RedisServerPools {
	//此属性可能不需要,两者都是全局单例对象
	private Redis_KeySetDomain redis_KeySet_defDomain;
	private List<KeyServerPools> keyServerPoolsList = new ArrayList<KeyServerPools>();

	//解析key属于哪个阵列的redis集群组
	public KeyServerPools getKeyServerPools(String key) {
		
		/*for (KeyServerPools keyServerPools : this.keyServerPoolsList){
			Pattern pattern = Pattern.compile(keyServerPools.getRedis_KeyPattern_defDomain().getPattern(),Pattern.CASE_INSENSITIVE);
			if (pattern.matcher(key).matches()) {
				return keyServerPools;
			}
		}*/
		//改为hash算法 ,lff修改20130829
		if(keyServerPoolsList !=null && keyServerPoolsList.size()>0){
			int hashCode = key.hashCode();
			if(hashCode<0){
				hashCode = -hashCode;
			}
			int index = hashCode%keyServerPoolsList.size();
			//合法性验证
			while(!this.isValidKeyServerPools(keyServerPoolsList.get(index))){
				if(index==keyServerPoolsList.size()-1){
					index = 0;
				}else{
					index++;
				}
			}
			return keyServerPoolsList.get(index);
		}
		return null;
	}
	//解析key属于哪个阵列的redis集群组对应的索引 add by sunJiaming 20140417
	public int getKeyServerPoolsIndex(String key) {
		if(keyServerPoolsList !=null && keyServerPoolsList.size()>0){
			int hashCode = key.hashCode();
			if(hashCode<0){
				hashCode = -hashCode;
			}
			int index = hashCode%keyServerPoolsList.size();
			//合法性验证
			while(!this.isValidKeyServerPools(keyServerPoolsList.get(index))){
				if(index==keyServerPoolsList.size()-1){
					index = 0;
				}else{
					index++;
				}
			}
			return index;
		}
		return 0;
		//改为主备，sunJiaming修改20140818
		/*if(keyServerPoolsList !=null && keyServerPoolsList.size()>0){
			int index = 0;
			//合法性验证
			while(!this.isValidKeyServerPools(keyServerPoolsList.get(index))){
				index = 1;
				break;
			}
			return index;
		}
		return 0;*/
	}
	//解析写组索引属于哪个阵列的redis集群组 add by sunJiaming 20140417
	public KeyServerPools getKeyServerPoolsByIndex(int index) {
		return keyServerPoolsList.get(index);
	}
	/**
	 * 获得有效的
	 * @param key
	 * @return
	 */
	public boolean isValidKeyServerPools(KeyServerPools keyServerPools){
		if(keyServerPools.getWriterPools().size()>0){
			return true;
		}
		return false;
	}
//根据key获取一个阵列的写组
	public List<RedisPool> getWriterPools(String key) {
		List<RedisPool> writerPools = new ArrayList<RedisPool>(); 
		KeyServerPools keyServerPools = this.getKeyServerPools(key);
		if (keyServerPools != null) {
			writerPools.addAll(keyServerPools.getWriterPools());
		}
		return writerPools;
	}
//根据写组的索引获取一个阵列的写组 add by sunJiaming 20140417
	public List<RedisPool> getWriterPoolsByIndex(int index) {
		List<RedisPool> writerPools = new ArrayList<RedisPool>(); 
		KeyServerPools keyServerPools = this.getKeyServerPoolsByIndex(index);
		if (keyServerPools != null) {
			writerPools.addAll(keyServerPools.getWriterPools());
		}
		return writerPools;
	}
//根据KEY获取一个阵列的读组,如果没有读组就获取写组
	public List<RedisPool> getReaderPools(String key) {
		List<RedisPool> readerPools = new ArrayList<RedisPool>(); 
		KeyServerPools keyServerPools = this.getKeyServerPools(key);
		if (keyServerPools == null)
			return null;
		List<RedisPool> readList = new ArrayList<RedisPool>();
		readList.addAll(keyServerPools.getReadPools());
		if (readList != null && readList.size() > 0) {
			//todo,还需要检查里面的是否都可以死亡,如果这里的读都死亡了应该提早返回writePools
			for(RedisPool redisPool: readList){
				if(!redisPool.isDead()){
					readerPools.add(redisPool);
				}
			}
		}  
		List<RedisPool> writerList = new ArrayList<RedisPool>();
		writerList.addAll(keyServerPools.getWriterPools());
		if(readerPools.size()<1 && writerList !=null && writerList.size()>0){
			//todo,检查里面的是否死亡
//			return keyServerPools.getWriterPools();
			for(RedisPool writerPool : writerList){
				readerPools.add(writerPool);
			}
		}
		return readerPools;
	}
//获取所有阵列的读写组，只在销毁的时候用
	public List<RedisPool> getAllPools() {
		List<RedisPool> pools = new ArrayList<RedisPool>();
		for (KeyServerPools keyServerPools : this.keyServerPoolsList) {
			if (keyServerPools.getReadPools() != null) {
				pools.addAll(keyServerPools.getReadPools());
			}
			if (keyServerPools.getWriterPools() != null) {
				pools.addAll(keyServerPools.getWriterPools());
			}
		}
		return pools;
	}
	
	public Redis_KeySetDomain getRedis_KeySet_defDomain() {
		return redis_KeySet_defDomain;
	}

	public void setRedis_KeySet_defDomain(
			Redis_KeySetDomain redis_KeySet_defDomain) {
		this.redis_KeySet_defDomain = redis_KeySet_defDomain;
	}

	public List<KeyServerPools> getKeyServerPoolsList() {
		return keyServerPoolsList;
	}

	public void setKeyServerPoolsList(List<KeyServerPools> keyServerPoolsList) {
		this.keyServerPoolsList = keyServerPoolsList;
	}

}
