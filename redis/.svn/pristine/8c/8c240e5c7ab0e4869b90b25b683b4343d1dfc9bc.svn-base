package org.dacell.redis.domain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Redis_KeyPatternDomain {

	private String id;
	private String pattern;
	private List<Redis_ServerDomain> redisServerList = new ArrayList<Redis_ServerDomain>();// 该规则对应的读写服务器组的所有实例
	private String description;

	public void addRedisServer(Redis_ServerDomain redisServer) {
		Iterator<Redis_ServerDomain> iterator = redisServerList
				.iterator();
		boolean flag = true;
		while (iterator.hasNext()) {
			Redis_ServerDomain server = iterator.next();
			if (redisServer.equals(server)) {
				flag = false;
				break;
			}
		}
		if (flag) {
			redisServerList.add(redisServer);
		}
	}

	// public void removeRedisServer(Redis_Server_instanceDomain redisServer) {
	// Iterator<Redis_Server_instanceDomain> iterator =
	// redisServerList.iterator();
	// while (iterator.hasNext()){
	// Redis_Server_instanceDomain server = iterator.next();
	// if (redisServer.equals(server)) {
	// iterator.remove();
	// }
	// }
	// }

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Redis_ServerDomain> getRedisServerList() {
		return redisServerList;
	}

	public void setRedisServerList(
			List<Redis_ServerDomain> redisServerList) {
		this.redisServerList = redisServerList;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
