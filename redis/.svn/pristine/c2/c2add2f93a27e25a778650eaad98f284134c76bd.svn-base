package org.dacell.redis.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Redis_KeySetDomain {
	
	private static Redis_KeySetDomain redis_KeySetDomain = null;
	private static final List<Redis_KeyPatternDomain> keyPatternList=new ArrayList<Redis_KeyPatternDomain>();
	//设置规则集合的默认过期时间

	
	private static Map<String,String> serverExecuteParam;

	public static void addServerExecuteParam(String name,String value){
		if(Redis_KeySetDomain.serverExecuteParam == null){
			Redis_KeySetDomain.serverExecuteParam = new HashMap<String,String>();
		}
		Redis_KeySetDomain.serverExecuteParam.put(name,value);
	}
	
	public static Object getServerExecuteParam(String key) {
		return serverExecuteParam.get(key);
	}

	private Redis_KeySetDomain(){
	}
	
	public static Redis_KeySetDomain getInstance() {
		if (redis_KeySetDomain == null) {
			redis_KeySetDomain = new Redis_KeySetDomain();
		}
		return redis_KeySetDomain;
	}
	
	public void addKeyPattern(Redis_KeyPatternDomain redis_KeyPattern_defDomain){
		keyPatternList.add(redis_KeyPattern_defDomain);
	}

	public List<Redis_KeyPatternDomain> getKeyPatternList() {
		return keyPatternList;
	}

//	public static Long getEXCEPTION_COUNT_IN_SCOPE() {
//		return EXCEPTION_COUNT_IN_SCOPE;
//	}
//
//	public static int getINTERVALTIME() {
//		return INTERVALTIME;
//	}
//
//	public static int getTEST_CONNECTION_COUNT() {
//		return TEST_CONNECTION_COUNT;
//	}
//
//	public static Long getDefault_Expire_seconds() {
//		return default_expire_seconds;
//	}
//
//	public static void setDefault_expire_seconds(Long default_expire_seconds) {
//		Redis_KeySetDomain.default_expire_seconds = default_expire_seconds;
//	}

}
