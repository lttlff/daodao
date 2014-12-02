package org.dacell.redis.domain;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Redis_ServerDomain {

	private String id;
	private String name;
	private String host;
	private Long port;
	private Map<String,String> serverAttrMap;
	private Date startTime;

	public void addServerAttr(String name,String value){
		if(this.serverAttrMap == null){
			this.serverAttrMap = new HashMap<String,String>();
		}
		this.serverAttrMap.put(name,value);
	}
	
	public Map<String, String> getServerAttrMap() {
		return serverAttrMap;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public Long getPort() {
		return port;
	}

	public void setPort(Long port) {
		this.port = port;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}

}
