package org.dacell.redis.component.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class NodeletParser {
	private Map letMap = new HashMap();
	private boolean validation;
	private EntityResolver entityResolver;

	public void addNodelet(String xpath, Nodelet nodelet) {
		this.letMap.put(xpath, nodelet);
	}

	public static void main(String[] args) throws NodeletException {
		NodeletParser parser = new NodeletParser();
		parser.addSweb(parser);
		parser.addSwebWelcomeFile(parser);
		parser.addResourcesModel(parser);
		parser.parse(NodeletParser.class.getClassLoader().getResourceAsStream(
				"sweb-config-detail.xml"));
		Map map = parser.letMap;
	}

	private void addSweb(NodeletParser parser) {
		parser.addNodelet("/sweb", new Nodelet() {
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				System.out.println(map.getNamedItem("encode").getNodeValue());
			}
		});
	}

	private void addSwebWelcomeFile(NodeletParser parser) {
		parser.addNodelet("/sweb/welcome-file", new Nodelet() {
			public void process(Node node) throws Exception {
//				System.out.println(node.getTextContent().trim());
			}
		});
	}

	private void addResourcesModel(NodeletParser parser) {
		parser.addNodelet("/sweb/resources/resource", new Nodelet() {
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				System.out.println("[/sweb/resources/resource] name=["
						+ map.getNamedItem("name").getNodeValue() + "]");
			}
		});
		parser.addNodelet("/sweb/resources/resource/param", new Nodelet() {
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				System.out.println("[/sweb/resources/resource/param] name=["
						+ map.getNamedItem("name").getNodeValue() + "] value=["
						+ map.getNamedItem("value").getNodeValue() + "]");
			}
		});
		parser.addNodelet("/sweb/resources/resource/end()", new Nodelet() {
			public void process(Node node) throws Exception {
				NamedNodeMap map = node.getAttributes();
				System.out.println("[/sweb/resources/resource] name=["
						+ NodeletUtils.getNodeValue(map, "name1") + "]");
			}
		});
	}

	public void parse(Reader reader) throws NodeletException {
		try {
			Document doc = createDocument(reader);
			parse(doc.getLastChild());
		} catch (Exception e) {
			throw new NodeletException("Error parsing XML.  Cause: " + e, e);
		}
	}

	public void parse(InputStream inputStream) throws NodeletException {
		try {
			Document doc = createDocument(inputStream);
			parse(doc.getLastChild());
		} catch (Exception e) {
			throw new NodeletException("Error parsing XML.  Cause: " + e, e);
		}
	}

	public void parse(Node node) {
		Path path = new Path();
		processNodelet(node, "/");
		process(node, path);
	}

	private void process(Node node, Path path) {
		if ((node instanceof Element)) {
			String elementName = node.getNodeName();
			path.add(elementName);
			processNodelet(node, path.toString());
			processNodelet(node, "//" + elementName);

			NamedNodeMap attributes = node.getAttributes();
			int n = attributes.getLength();
			for (int i = 0; i < n; i++) {
				Node att = attributes.item(i);
				String attrName = att.getNodeName();
				path.add("@" + attrName);
				processNodelet(att, path.toString());
				processNodelet(node, "//@" + attrName);
				path.remove();
			}

			NodeList children = node.getChildNodes();
			for (int i = 0; i < children.getLength(); i++) {
				process(children.item(i), path);
			}
			path.add("end()");
			processNodelet(node, path.toString());
			path.remove();
			path.remove();
		} else if ((node instanceof Text)) {
			path.add("text()");
			processNodelet(node, path.toString());
			processNodelet(node, "//text()");
			path.remove();
		}
	}

	private void processNodelet(Node node, String pathString) {
		Nodelet nodelet = (Nodelet) this.letMap.get(pathString);
		if (nodelet != null)
			try {
				nodelet.process(node);
			} catch (Exception e) {
				throw new RuntimeException("Error parsing XPath '" + pathString
						+ "'.  Cause: " + e, e);
			}
	}

	private Document createDocument(Reader reader)
			throws ParserConfigurationException, FactoryConfigurationError,
			SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(this.validation);

		factory.setNamespaceAware(false);
		factory.setIgnoringComments(true);
		factory.setIgnoringElementContentWhitespace(false);
		factory.setCoalescing(false);
		factory.setExpandEntityReferences(true);

		DocumentBuilder builder = factory.newDocumentBuilder();
		builder.setEntityResolver(this.entityResolver);
		builder.setErrorHandler(new ErrorHandler() {
			public void error(SAXParseException exception) throws SAXException {
				throw exception;
			}

			public void fatalError(SAXParseException exception)
					throws SAXException {
				throw exception;
			}

			public void warning(SAXParseException exception)
					throws SAXException {
			}
		});
		return builder.parse(new InputSource(reader));
	}

	private Document createDocument(InputStream inputStream)
			throws ParserConfigurationException, FactoryConfigurationError,
			SAXException, IOException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(this.validation);

		factory.setNamespaceAware(false);
		factory.setIgnoringComments(true);
		factory.setIgnoringElementContentWhitespace(false);
		factory.setCoalescing(false);
		factory.setExpandEntityReferences(true);

		DocumentBuilder builder = factory.newDocumentBuilder();
		builder.setEntityResolver(this.entityResolver);
		builder.setErrorHandler(new ErrorHandler() {
			public void error(SAXParseException exception) throws SAXException {
				throw exception;
			}

			public void fatalError(SAXParseException exception)
					throws SAXException {
				throw exception;
			}

			public void warning(SAXParseException exception)
					throws SAXException {
			}
		});
		return builder.parse(new InputSource(inputStream));
	}

	public void setValidation(boolean validation) {
		this.validation = validation;
	}

	public void setEntityResolver(EntityResolver resolver) {
		this.entityResolver = resolver;
	}

	private static class Path {
		private List nodeList = new ArrayList();

		public Path() {
		}

		public Path(String path) {
			StringTokenizer parser = new StringTokenizer(path, "/", false);
			while (parser.hasMoreTokens())
				this.nodeList.add(parser.nextToken());
		}

		public void add(String node) {
			this.nodeList.add(node);
		}

		public void remove() {
			this.nodeList.remove(this.nodeList.size() - 1);
		}

		public String toString() {
			StringBuffer buffer = new StringBuffer("/");
			for (int i = 0; i < this.nodeList.size(); i++) {
				buffer.append(this.nodeList.get(i));
				if (i < this.nodeList.size() - 1) {
					buffer.append("/");
				}
			}
			return buffer.toString();
		}
	}
}
