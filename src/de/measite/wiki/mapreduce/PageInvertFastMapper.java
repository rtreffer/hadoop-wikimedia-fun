/**
 * 
 */
package de.measite.wiki.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.measite.wiki.mapreduce.io.PageInvertWritable;

public class PageInvertFastMapper extends
Mapper<Object, Text, Text, PageInvertWritable> {

	private final static SimpleDateFormat dateFormat = new SimpleDateFormat(
	"yyyy-MM-dd HH:mm:ss", Locale.US);

	@Override
	protected synchronized void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		if (value == null || value.getBytes() == null) {
			return;
		}
		String content = new String(value.getBytes());

		String title = extractTitle(content);
		if (title == null) {
			return;
		}

		// Step 1 - split all authors

		double revisionScore = 1D / (Math.log1p(count(content, "<revision>")
		* 2 - count(content, "<minor/>")));

		int revisionPos = content.indexOf("<revision>");
		while (revisionPos != -1) {
			int revisionEnd = content.indexOf("</revision>");
			if (revisionEnd == -1) {
				revisionPos = -1;
				continue;
			}
			String revision = content.substring(revisionPos + 10, revisionEnd);
			content = content.substring(revisionEnd + 11);
			revisionPos = content.indexOf("<revision>");

			String contributor = extractContributor(revision);
			if (contributor == null) {
				continue;
			}

			String username = extractUsername(contributor);
			if (username == null) {
				continue;
			}

			Text outKey = new Text("U" + username);

			PageInvertWritable outValue = new PageInvertWritable();
			outValue.setSource(title);
			outValue.setTimestamp(extractTimestamp(revision));
			if (revision.indexOf("<minor/>") == -1) {
				outValue.setScore(2 * revisionScore);
			} else {
				outValue.setScore(revisionScore);
			}
			context.write(outKey, outValue);
		}

		// Step 2, split the words...

		String text = extractText(content);
		pushMap(title, 'W', scoreWords(extractWords(text)), context);

		// Step 3, outgoing links...

		pushMap(title, 'L', scoreWords(extractLinks(text)), context);
	}

	private String extractText(String content) {
		int startPos = content.indexOf("<text xml:space=\"preserve\">");
		int endPos = content.indexOf("</text>");
		if (startPos == -1 || endPos == -1) {
			return "";
		}
		return content.substring(startPos + 27, endPos).trim();
	}

	private long extractTimestamp(String revision) {
		int startPos = revision.indexOf("<timestamp>");
		if (startPos == -1) {
			return -1l;
		}
		int endPos = revision.indexOf("</timestamp>");

		String date = revision.substring(startPos + 11, endPos);
		date = date.replace('T', ' ');
		date = date.replace('Z', ' ');
		date = date.trim();
		try {
			return dateFormat.parse(date).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return -1l;
	}

	private int count(String text, String match) {
		int count = 0;
		int index = -1;
		while ((index = text.indexOf(match, index)) != -1) {
			index += match.length();
			count++;
		}
		return count;
	}

	private String extractUsername(String revision) {
		int startPos = revision.indexOf("<username>");
		int endPos = -1;
		if (startPos != -1) {
			endPos = revision.indexOf("</username>");
		} else {
			startPos = revision.indexOf("<id>");
			if (startPos != -1) {
				endPos = revision.indexOf("</id>");
			} else {
				startPos = revision.indexOf("<ip>");
				endPos = revision.indexOf("</ip>");
			}
		}
		if (startPos == -1) {
			return null;
		}
		String result = revision.substring(startPos, endPos);
		for (int i = 0; i < result.length(); i++) {
			// Filter ip's, id's and nonsense
			if (Character.isLetter(result.charAt(i))) {
				return result;
			}
		}
		return null;
	}

	private String extractContributor(String revision) {
		int startPos = revision.indexOf("<contributor>");
		if (startPos == -1) {
			return null;
		}
		int endPos = revision.indexOf("</contributor>");
		return revision.substring(startPos + 13, endPos).trim();
	}

	private String extractTitle(String content) {
		int startPos = content.indexOf("<title>");
		int endPos = content.indexOf("</title>", startPos + 7);
		if (startPos == -1) {
			return null;
		}
		return content.substring(startPos + 7, endPos).trim();
	}

	private void pushMap(String source, char prefix, Map<String, Double> map, Context context)
	throws IOException, InterruptedException {
		for (String word : map.keySet()) {
			MapWritable outValue = new MapWritable();
			outValue.setConf(context.getConfiguration());

			Text outKey = new Text(prefix + word);

			context.write(outKey,
			new PageInvertWritable(source, map.get(word)));
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
					result.add(sb.toString().toLowerCase());
				}
				sb.setLength(0);
			}
		}
		return result;
	}

	private Map<String, Double> scoreWords(List<String> words) {
		Map<String, Double> tresult = new HashMap<String, Double>();
		for (String word : words) {
			Double score = tresult.get(word);
			if (score != null) {
				tresult.put(word, score + 1d);
			} else {
				tresult.put(word, 1d);
			}
		}
		Map<String, Double> result = new HashMap<String, Double>(
		tresult.size() * 2);
		double div = 1d / Math.log1p(words.size());
		for (String word : tresult.keySet().toArray(new String[0])) {
			result.put(word, Math.log1p(tresult.remove(word)) * div);
		}
		return result;
	}

}