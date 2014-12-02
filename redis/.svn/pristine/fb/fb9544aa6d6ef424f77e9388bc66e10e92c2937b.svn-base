package org.dacell.redis.cfg;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import org.dacell.redis.component.xml.Nodelet;
import org.dacell.redis.component.xml.NodeletException;
import org.dacell.redis.component.xml.NodeletParser;
import org.dacell.redis.component.xml.NodeletUtils;
import org.dacell.redis.domain.Redis_KeyPatternDomain;
import org.dacell.redis.domain.Redis_KeySetDomain;
import org.dacell.redis.domain.Redis_ServerDomain;

public class RpcXmlConfigure {

	private static Redis_KeySetDomain redis_KeySet;

	private static ThreadLocal<Redis_ServerDomain> redisServerThreadLocal = new ThreadLocal<Redis_ServerDomain>();
	private static ThreadLocal<Redis_KeyPatternDomain> KeyPattern = new ThreadLocal<Redis_KeyPatternDomain>();

	static {
		redis_KeySet = Redis_KeySetDomain.getInstance();
		NodeletParser parser = new NodeletParser();
		addKeySetAndRedisServersParser(parser);
		try {
			InputStream clientIs = NodeletParser.class.getClassLoader()
					.getResourceAsStream("redis_server.xml");
			if (clientIs != null) {
				parser.parse(clientIs);
				try {
					clientIs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} catch (NodeletException e) {
			e.printStackTrace();
		}
	}

	private static void addKeySetAndRedisServersParser(NodeletParser parser) {
		
		parser.addNodelet("/redis_server", new Nodelet() {
			@SuppressWarnings("static-access")
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				
				String excepnum_to_death = NodeletUtils.getNodeValue(map, "excepnum_to_death");
				if (excepnum_to_death == null || "".equalsIgnoreCase(excepnum_to_death.trim())) {
					excepnum_to_death="10";
				}
				
				String scan_interval_misecond = NodeletUtils.getNodeValue(map, "scan_interval_misecond");
				if (scan_interval_misecond == null || "".equalsIgnoreCase(scan_interval_misecond.trim())) {
					scan_interval_misecond="60000";
				}
				
				String test_connection_count = NodeletUtils.getNodeValue(map, "test_connection_count");
				if (test_connection_count == null || "".equalsIgnoreCase(test_connection_count.trim())) {
					test_connection_count="3";
				}
				
				String default_expire_seconds = NodeletUtils.getNodeValue(map, "default_expire_seconds");
				if (default_expire_seconds == null || "".equalsIgnoreCase(default_expire_seconds.trim())) {
					default_expire_seconds="1800";
				}
				
				redis_KeySet.addServerExecuteParam("excepnum_to_death", excepnum_to_death);
				redis_KeySet.addServerExecuteParam("scan_interval_misecond", scan_interval_misecond);
				redis_KeySet.addServerExecuteParam("test_connection_count", test_connection_count);
				redis_KeySet.addServerExecuteParam("default_expire_seconds", default_expire_seconds);
			}
		});
		
		parser.addNodelet("/redis_server/servers", new Nodelet() {
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				String pattern = NodeletUtils.getNodeValue(map, "keypattern");
				String description = NodeletUtils.getNodeValue(map, "desc");
				Redis_KeyPatternDomain keyPattern = new Redis_KeyPatternDomain();
				keyPattern.setPattern(pattern);
				keyPattern.setDescription(description);
				redis_KeySet.addKeyPattern(keyPattern);
				
				KeyPattern.set(keyPattern);
			}
		});

		parser.addNodelet("/redis_server/servers/server", new Nodelet() {
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				String name = NodeletUtils.getNodeValue(map, "name");
				String host = NodeletUtils.getNodeValue(map, "host");
				if (host == null || "".equalsIgnoreCase(host.trim())) {
					return;
				}
				String port = NodeletUtils.getNodeValue(map, "port");
				if (port == null || "".equalsIgnoreCase(port.trim())){
					return;
				}
				Redis_ServerDomain redisServer = new Redis_ServerDomain();
				redisServer.setName(name);
				redisServer.setHost(host);
				redisServer.setPort(Long.valueOf(port));
				
				Redis_KeyPatternDomain keyPatternDomain=KeyPattern.get();
				keyPatternDomain.addRedisServer(redisServer);
				
				redisServerThreadLocal.set(redisServer);
			}
		});

		parser.addNodelet("/redis_server/servers/server/param", new Nodelet() {
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				String name = NodeletUtils.getNodeValue(map, "name");
				String value = NodeletUtils.getNodeValue(map, "value");

				Redis_ServerDomain redisServerAttr = (Redis_ServerDomain) redisServerThreadLocal
						.get();
				redisServerAttr.addServerAttr(name, value);
			}
		});
		parser.addNodelet("/redis_server/servers/server/param/end()",
				new Nodelet() {
					public void process(Node node) throws Exception {
						// pass
					}
				});
	}
	
	public static Redis_KeySetDomain getRedis_KeySet() {
		return redis_KeySet;
	}
	
	public static void main(String[] args) {
		RpcXmlConfigure cif = new RpcXmlConfigure();
		for (Redis_KeyPatternDomain keypattern : (List<Redis_KeyPatternDomain>) redis_KeySet
				.getInstance().getKeyPatternList()) {
//			System.out.println("=" + keypattern.getDescription());
			for (Redis_ServerDomain server : keypattern
					.getRedisServerList()) {
				System.out.println("server name maxIdle=" + server.getServerAttrMap().get("maxIdle"));
				System.out.println("server name maxIdle=" + server.getPort());
			}
		}
	}
}
