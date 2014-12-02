package org.dacell.redis.component.xml;

import java.util.Properties;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

public class NodeletUtils {
	public static String getNodeValue(NamedNodeMap map, String name) {
		try {
			return map.getNamedItem(name).getNodeValue();
		} catch (NullPointerException e) {
		}
		return null;
	}

	public static boolean getBooleanAttribute(Properties attribs, String name,
			boolean def) {
		String value = attribs.getProperty(name);
		if (value == null) {
			return def;
		}
		return "true".equals(value);
	}

	public static int getIntAttribute(Properties attribs, String name, int def) {
		String value = attribs.getProperty(name);
		if (value == null) {
			return def;
		}
		return Integer.parseInt(value);
	}

	public static Properties parseAttributes(Node n) {
		return parseAttributes(n, null);
	}

	public static Properties parseAttributes(Node n, Properties variables) {
		Properties attributes = new Properties();
		NamedNodeMap attributeNodes = n.getAttributes();
		for (int i = 0; i < attributeNodes.getLength(); i++) {
			Node attribute = attributeNodes.item(i);
			String value = parsePropertyTokens(attribute.getNodeValue(),
					variables);
			attributes.put(attribute.getNodeName(), value);
		}
		return attributes;
	}

	public static String parsePropertyTokens(String string, Properties variables) {
		String OPEN = "${";
		String CLOSE = "}";

		String newString = string;
		if ((newString != null) && (variables != null)) {
			int start = newString.indexOf("${");
			int end = newString.indexOf("}");

			while ((start > -1) && (end > start)) {
				String prepend = newString.substring(0, start);
				String append = newString.substring(end + "}".length());
				String propName = newString.substring(start + "${".length(),
						end);
				String propValue = variables.getProperty(propName);
				if (propValue == null)
					newString = prepend + propName + append;
				else {
					newString = prepend + propValue + append;
				}
				start = newString.indexOf("${");
				end = newString.indexOf("}");
			}
		}
		return newString;
	}
}
