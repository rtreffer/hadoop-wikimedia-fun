package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.measite.wiki.mapreduce.io.LinkWritable;
import de.measite.wiki.mapreduce.io.PageInvertWritable;

public class LinkInverterMapper extends
Mapper<Text, PageInvertWritable, Text, LinkWritable> {

	@Override
	protected void map(Text key, PageInvertWritable value, Context context) throws IOException,
	InterruptedException {
		String k = new String(key.getBytes());
		if (!k.startsWith("L[[")) {
			return;
		}
		k.substring(3, k.length() - 2);
		String v = value.getSource();
		Text key1 = new Text(k);
		Text key2 = new Text(v);
		// Link (backward, implicit, 75% score)
		LinkWritable bValue = new LinkWritable(k, v, value.getScore() * 0.75);
		// Link (forward)
		LinkWritable fValue = new LinkWritable(v, k, value.getScore());
		// Write
		context.write(key1, fValue);
		context.write(key1, bValue);
		context.write(key2, fValue);
		context.write(key2, bValue);
	}

}
