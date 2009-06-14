/**
 * 
 */
package de.measite.wiki.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.measite.wiki.mapreduce.io.PageInvertWriteable;

public class PageInvertMapper extends
Mapper<Object, Text, Text, PageInvertWriteable> {

	public final static Text TIMESTAMP = new Text("T");
	public final static Text SOURCE = new Text("F");
	public final static Text SCORE = new Text("S");

	private final static DocumentBuilderFactory factory;
	private final static XPathFactory xpFactory;
	private final XPathExpression xpTitle;
	private final XPathExpression xpText;
	private final XPathExpression xpRevisions;
	private final XPathExpression xpTimestamp;
	private final XPathExpression xpUsername;
	private final XPathExpression xpUserid;
	private final XPathExpression xpUserIP;
	private final XPathExpression xpMinor;
	private final static SimpleDateFormat dateFormat = new SimpleDateFormat(
	"yyyy-MM-dd HH:mm:ss", Locale.US);

	static {
		factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		xpFactory = XPathFactory.newInstance();
	}

	public PageInvertMapper() {
		XPathExpression title = null;
		XPathExpression text = null;
		XPathExpression revisions = null;
		XPathExpression timestamp = null;
		XPathExpression username = null;
		XPathExpression userid = null;
		XPathExpression userip = null;
		XPathExpression minor = null;
		try {
			title = xpFactory.newXPath().compile("//page/title");
			text = xpFactory.newXPath().compile("//page/text");
			revisions = xpFactory.newXPath().compile("//page/revision");
			timestamp = xpFactory.newXPath().compile("timestamp");
			username = xpFactory.newXPath().compile("contributor/username");
			userid = xpFactory.newXPath().compile("contributor/id");
			userip = xpFactory.newXPath().compile("contributor/ip");
			minor = xpFactory.newXPath().compile("minor");
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
		xpTitle = title;
		xpText = text;
		xpRevisions = revisions;
		xpTimestamp = timestamp;
		xpUsername = username;
		xpUserid = userid;
		xpUserIP = userip;
		xpMinor = minor;
	}

	@Override
	protected synchronized void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		if (value == null || value.getBytes() == null) {
			return;
		}
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			Document d = builder.parse(new ByteArrayInputStream(value
			.getBytes()));

			Node node = (Node) xpTitle.evaluate(d, XPathConstants.NODE);

			String title = "[[" + node.getTextContent().trim() + "]]";

			// Step 1 - split all authors

			NodeList revisions = (NodeList) xpRevisions.evaluate(d,
			XPathConstants.NODESET);
			long total = 0;
			for (int i = 0; i < revisions.getLength(); i++) {
				node = revisions.item(i);
				NodeList minors = (NodeList) xpMinor.evaluate(node,
				XPathConstants.NODESET);
				if (minors.getLength() == 0) {
					total += 2;
				} else {
					total += 1;
				}
			}

			for (int i = 0; i < revisions.getLength(); i++) {

				PageInvertWriteable outValue = new PageInvertWriteable();
				outValue.setSource(title);

				node = revisions.item(i);
				NodeList minors = (NodeList) xpMinor.evaluate(node,
				XPathConstants.NODESET);
				if (minors.getLength() == 0) {
					outValue.setScore(2D / total);
				} else {
					outValue.setScore(1D / total);
				}
				Text outKey = new Text();
				NodeList usernames = (NodeList) xpUsername.evaluate(node,
				XPathConstants.NODESET);
				if (usernames.getLength() == 0) {
					NodeList userids = (NodeList) xpUserid.evaluate(node,
					XPathConstants.NODESET);
					if (userids.getLength() == 0) {
						NodeList userips = (NodeList) xpUserIP.evaluate(
						node, XPathConstants.NODESET);
						if (userips.getLength() == 0) {
							System.err
							.println("--- O.o no user found --- Record o.O ---");
							System.err.println(value.toString());
						}
						continue;
					} else {
						outKey.set("[[User:" + userids.item(0).getTextContent() + "]]");
					}
				} else {
					outKey.set("[[User:" + usernames.item(0).getTextContent() + "]]");
				}
				long timestamp = -1L;
				NodeList timestamps = (NodeList) xpTimestamp.evaluate(node,
				XPathConstants.NODESET);
				if (timestamps.getLength() != 0) {
					String date = timestamps.item(0).getTextContent();
					date = date.replace('T', ' ');
					date = date.replace('Z', ' ');
					date = date.trim();
					try {
						timestamp = dateFormat.parse(date).getTime();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				outValue.setTimestamp(timestamp);
				context.write(outKey, outValue);
			}

			// Step 2, split the words...

			NodeList texts = (NodeList) xpText.evaluate(node,
			XPathConstants.NODESET);
			if (texts.getLength() == 0) {
				return;
			}
			String text = texts.item(0).getTextContent();
			pushMap(title, scoreWords(extractWords(text)), context);

			// Step 3, outgoing links...

			List<String> extractedLinks = extractLinks(text);
			for (String link: extractedLinks.toArray(new String[0])) {
				if (link.startsWith("[[User:")) {
					extractedLinks.remove(link);
				}
			}
			pushMap(title, scoreWords(extractedLinks), context);

		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (XPathExpressionException e) {
			e.printStackTrace();
		}
	}

	private void pushMap(String source, Map<String, Double> map, Context context) throws IOException, InterruptedException {
		for (String word: map.keySet()) {
			MapWritable outValue = new MapWritable();
			outValue.setConf(context.getConfiguration());

			Text outKey = new Text(word);

			context.write(outKey, new PageInvertWriteable(source, map.get(word)));
		}
	}

	private List<String> extractLinks(String text) {
		List<String> result = new ArrayList<String>();
		int start = text.indexOf("[[");
		while (start != -1) {
			text = text.substring(start + 2);
			int end = text.indexOf("]]");
			if (end == -1) {
				start = -1;
			} else {
				int pipe = text.substring(0, end).indexOf('|');
				if (pipe != -1) {
					end = pipe;
				}
				result.add("[[" + text.substring(0, end) + "]]");
				text = text.substring(end);
				start = text.indexOf("[[");
			}
		}
		return result;
	}

	private List<String> extractWords(String text) {
		List<String> result = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if (Character.isLetterOrDigit(c)) {
				sb.append(c);
			} else if (sb.length() > 0) {
				if (sb.length() > 3) {
					result.add(sb.toString());
				}
				sb.setLength(0);
			}
		}
		return result;
	}

	private Map<String, Double> scoreWords(List<String> words) {
		Map<String, Double> tresult = new HashMap<String, Double>();
		for (String word: words) {
			Double score = tresult.get(word);
			if (score != null) {
				tresult.put(word, score + 1d);
			} else {
				tresult.put(word, 1d);
			}
		}
		Map<String, Double> result = new HashMap<String, Double>(tresult.size() * 2);
		double div = 1d / Math.log1p(words.size());
		for (String word: tresult.keySet().toArray(new String[0])) {
			result.put(word, Math.log1p(tresult.remove(word)) * div);
		}
		return result;
	}

}