package de.measite.wiki.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import de.measite.wiki.input.WikimediaSimplifyInputFormat;

/**
 * This is the M/R we've been heading for. InputReader: read/split/simplify the
 * input Map: Invert the content (reverse links, text to page, edits by user)
 * Reduce: Take page lists based on text/user/links and compute pate-to-page
 * scores Map: Sum the page scores and
 */
public class PageRelation extends Configured implements Tool {

	private final static Text TIMESTAMP = new Text("T");
	private final static Text SOURCE = new Text("F");
	private final static Text SCORE = new Text("S");

	public static class PageInvertMapper extends
	Mapper<Object, Text, Text, MapWritable> {

		private final Text outKey = new Text();
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
		protected void map(Object key, Text value, Context context)
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

				Text title = new Text("[[" + node.getTextContent().trim() + "]]");

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

					MapWritable outValue = new MapWritable();
					outValue.setConf(context.getConfiguration());

					outValue.put(SOURCE, title);

					node = revisions.item(i);
					NodeList minors = (NodeList) xpMinor.evaluate(node,
					XPathConstants.NODESET);
					double score;
					if (minors.getLength() == 0) {
						outValue.put(SCORE, new DoubleWritable(2D / total));
					} else {
						outValue.put(SCORE, new DoubleWritable(1D / total));
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
					long timestamp = 0;
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
					} else {
						System.err
						.println("--- O.o no timestamp found --- Record o.O ---");
						System.err.println(value.toString());
					}
					outValue.put(TIMESTAMP, new LongWritable(timestamp));
					context.write(outKey, outValue);
				}
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (XPathExpressionException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PageRelation(),
		args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: pagecount <infile> <out>");
			System.exit(2);
		}
		conf.set("mapred.matchreader.record.start", "\n  <page>");
		conf.set("mapred.matchreader.record.end", "\n  </page>");
		conf.setLong("mapred.matchreader.record.maxSize", 180 * 1024 * 1024);
		try {
			Job job = new Job(conf, "page count");
			job.setJarByClass(PageRelation.class);
			job.setMapperClass(PageInvertMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(MapWritable.class);
			job.setInputFormatClass(WikimediaSimplifyInputFormat.class);
			job.setMapOutputValueClass(MapWritable.class);
			job.setMapOutputKeyClass(Text.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(job, args[0]);
			FileInputFormat.setMinInputSplitSize(job, 40 * 1024 * 1024);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.submit();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(3);
		}
		return 0;
	}

}
