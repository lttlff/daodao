package org.dacell.redis.component.xml;

public class NodeletException extends Exception {
	public NodeletException() {
	}

	public NodeletException(String msg) {
		super(msg);
	}

	public NodeletException(Throwable cause) {
		super(cause);
	}

	public NodeletException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
