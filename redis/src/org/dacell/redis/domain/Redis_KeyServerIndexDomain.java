package org.dacell.redis.domain;

public class Redis_KeyServerIndexDomain {
	private int serverIndex;
	private String key;
	private Object value;
	
	public Redis_KeyServerIndexDomain() {
	}
	public Redis_KeyServerIndexDomain(int serverIndex, String key,Object value) {
		this.serverIndex = serverIndex;
		this.key = key;
		this.value = value;
	}
	public int getServerIndex() {
		return serverIndex;
	}
	public void setServerIndex(int serverIndex) {
		this.serverIndex = serverIndex;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	
}
