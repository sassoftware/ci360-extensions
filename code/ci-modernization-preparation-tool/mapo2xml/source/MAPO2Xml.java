/*-----------------------------------------------------------------------------
 Copyright © 2024, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
 SPDX-License-Identifier: Apache-2.0
-----------------------------------------------------------------------------*/

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sas.analytics.crm.cm.client.CheckListInfo;
import com.sas.analytics.crm.cm.client.CheckListStepVO;
import com.sas.analytics.crm.cm.client.PageVO;
import com.sas.analytics.crm.persistence.objects.MAPO;
import com.sas.ci.services.common.persistence.serialization.SerialContent;
import com.sas.prompts.PromptValues;
import com.sas.services.information.metadata.ColumnAttributes;

public class MAPO2Xml {
	private static Document doc;
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) throws Exception {

		long t0 = System.currentTimeMillis();

		// Check for input arguments
		if (args.length < 2) {
			System.out.println("Usage java MAPO2Xml <user> <password> <url> <target XML file>");
			System.out.println("      java MAPO2Xml <input file> <target XML file>");
			return;
		}

		Object input;
		File target;
		String source;

		// Read serialized Java Object
		if (args.length < 4) {
			source = new File(args[0]).getAbsolutePath();
			input = readFileObject(source);
			target = new File(args[1]);
		} else {
			source = args[2];
			input = readURLObject(args[0], args[1], source);
			target = new File(args[3]);
		}
		long t1 = System.currentTimeMillis();
		System.out.println("read " + source + " in " + (t1 - t0) + " ms");

		// Convert to XML document
		doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		Element xml = object2xml(input, null);
		long t2 = System.currentTimeMillis();
		System.out.println("converted to xml in " + (t2 - t1) + " ms");

		// Write XML-file
		writeXmlFile(xml, target);
		long t3 = System.currentTimeMillis();
		System.out.println("wrote " + target.getAbsolutePath() + " in " + (t3 - t2) + " ms");
		System.out.println("total " + (t3 - t0) + " ms");
	}

	private static Object readFileObject(String source) throws Exception {
		File file = new File(source);
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
		Object o = ois.readObject();
		ois.close();
		return o;
	}

	public static Object readURLObject(String user, String pass, String url) throws Exception {
		System.setProperty("java.net.preferIPv4Stack", "true");
		String userpass = user + ":" + pass;
		String basic = "Basic " + DatatypeConverter.printBase64Binary(userpass.getBytes());
		URLConnection connection = new URI(url).toURL().openConnection();
		connection.setRequestProperty("Authorization", basic);
		ObjectInputStream ois = new ObjectInputStream(connection.getInputStream());
		Object o = ois.readObject();
		ois.close();
		return o;
	}

	public static void writeXmlFile(Element e, File outputFile) throws Exception {
		Transformer transformer = TransformerFactory.newInstance().newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "1");
		transformer.transform(new DOMSource(e), new StreamResult(outputFile));
	}

	private static Element object2xml(Object o, String nodeName) throws Exception {

		if (o == null)
			return null;
		if (o instanceof SerialContent)
			return serial2xml((SerialContent) o, nodeName);
		if (o instanceof MAPO)
			return mapo2xml((MAPO) o, nodeName);
		if (o instanceof Map)
			return map2xml((Map) o, nodeName);
		if (o instanceof List)
			return list2xml((List) o, nodeName);
		if (o instanceof ColumnAttributes)
			return columnAttributes2xml((ColumnAttributes) o, nodeName);
		if (o.getClass().isArray())
			return array2xml((Object[]) o, nodeName);
		if (o instanceof Date)
			return date2xml((Date) o, nodeName);
		if (o instanceof PromptValues)
			return prompt2xml((PromptValues) o, nodeName);
		if (o instanceof CheckListInfo)
			return list2xml(((CheckListInfo) o).getCheckList(), nodeName);
		if (o instanceof CheckListStepVO)
			return cls2xml((CheckListStepVO) o, nodeName);
		if (o instanceof PageVO)
			return page2xml((PageVO) o, nodeName);
		/*
		 * if (!(o instanceof String | o instanceof Boolean | o instanceof
		 * Integer | o instanceof Double | o instanceof Long))
		 * System.out.println(o.getClass() + ":" + o);
		 */
		String text = o.toString();
		if (text.isEmpty())
			return null;
		Element xml = newElement(o.getClass(), nodeName);
		xml.setTextContent(text);
		return xml;
	}

	private static Element prompt2xml(PromptValues prompt, String nodeName) throws Exception {
		Element xml = newElement(PromptValues.class, nodeName);
		Map values = prompt.getPromptValues();
		for (Object key : values.keySet())
			addChild(xml, object2xml(values.get(key), key.toString()));
		return xml;
	}

	private static Element date2xml(Date date, String nodeName) {
		Element xml = newElement(Date.class, nodeName);
		xml.setTextContent(dateFormat.format(date));
		return xml;
	}

	private static Element mapo2xml(MAPO mapo, String nodeName) throws Exception {
		Map properties = mapo.getProperties();
		Element xml = newElement(Class.forName(mapo.getClassName()), nodeName);
		addAttribute(xml, "id", mapo.getId());
		addAttribute(xml, "description", mapo.getDescription());
		addChild(xml, map2xml(properties, "properties"));

		return xml;
	}

	private static Element serial2xml(SerialContent serial, String nodeName) throws Exception {
		if (nodeName == null)
			nodeName = "SerialContent";
		Map attributes = serial.getAttributes();
		return map2xml(attributes, nodeName);
	}

	private static Element map2xml(Map map, String nodeName) throws Exception {
		if (map.isEmpty())
			return null;

		Object sortedKeys[] = map.keySet().toArray();
		Arrays.sort(sortedKeys);
		ArrayList<Element> containers = new ArrayList<Element>();
		Element xml = newElement(Map.class, nodeName);
		for (Object key : sortedKeys) {
			Object o = map.get(key);
			Element child = object2xml(o, key.toString());
			if (child != null) {
				if (o instanceof MAPO | o instanceof Map | o instanceof List
						| o instanceof SerialContent | o.getClass().isArray())
					containers.add(child);
				else
					addChild(xml, child);
			}
		}
		for (Element e : containers)
			addChild(xml, e);

		return xml;
	}

	private static Element columnAttributes2xml(ColumnAttributes ca, String nodeName) {
		Element xml = newElement(ColumnAttributes.class, null);
		addAttribute(xml, "columnType", "" + ca.getColumnType());
		addAttribute(xml, "columnLength", "" + ca.getColumnLength());
		xml.setTextContent(ca.getColumnName());

		return xml;
	}

	private static Element cls2xml(CheckListStepVO cls, String nodeName) {
		Element xml = newElement(CheckListStepVO.class, null);
		addAttribute(xml, "name", cls.name);
		addAttribute(xml, "status", cls.status);
		addAttribute(xml, "type", cls.type);
		addAttribute(xml, "id", cls.id);
		addAttribute(xml, "showAsLink", cls.showAsLink);
		return xml;
	}

	private static Element page2xml(PageVO page, String nodeName) {
		Element xml = newElement(PageVO.class, null);
		addAttribute(xml, "pageId", page.pageId);
		addAttribute(xml, "pageLabel", page.pageLabel);
		addAttribute(xml, "pageStatus", page.pageStatus);
		addAttribute(xml, "followupFlag", page.followupFlag);
		addAttribute(xml, "hiddenDefault", page.hiddenDefault);
		return xml;
	}

	private static Element list2xml(List list, String nodeName) throws Exception {
		if (list.isEmpty())
			return null;
		Element xml = newElement(List.class, nodeName);
		for (Object o : list)
			addChild(xml, object2xml(o, null));
		return xml;
	}

	private static Element array2xml(Object[] array, String nodeName) throws Exception {
		if (array.length == 0)
			return null;
		Element xml = newElement(Array.class, nodeName);
		for (Object o : array)
			addChild(xml, object2xml(o, null));
		return xml;
	}

	private static void addAttribute(Element xml, String name, Object value) {
		if (value == null)
			return;
		String s = value.toString().trim();
		if (!s.isEmpty())
			xml.setAttribute(name, s);
	}

	private static void addChild(Element parent, Element child) {
		if (child != null)
			parent.appendChild(child);
	}

	private static Element newElement(Class javaClass, String name) {
		Element xml;
		String className = javaClass.getSimpleName();
		String nodeName = (name == null) ? className : name.trim();
		try {
			xml = doc.createElement(nodeName);
		} catch (Exception exeception) {
			nodeName = "_" + nodeName.replaceAll("\\W", "_");
			xml = doc.createElement(nodeName);
		}
		if (name != null)
			xml.setAttribute("type", className);
		return xml;
	}
}
