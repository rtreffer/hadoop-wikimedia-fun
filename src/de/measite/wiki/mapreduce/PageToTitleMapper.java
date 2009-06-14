/**
 * 
 */
package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageToTitleMapper extends Mapper<Object, Text, Text, Text> {

	private final Text outKey = new Text();

	@Override
	protected void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		if (value == null) {
			return;
		}
		String text = value.toString();
		int pos = text.indexOf("\n    <title>");
		if (pos == -1) {
			return;
		}
		text = text.substring(pos + 12);
		pos = text.indexOf("</title>");
		outKey.set(text.substring(0, pos));
		context.write(outKey, value);
	}

}