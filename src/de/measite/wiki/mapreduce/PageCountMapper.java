/**
 * 
 */
package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageCountMapper extends Mapper<Object, Text, Text, LongWritable> {

	private final static LongWritable one = new LongWritable(1l);
	private final static Text page = new Text("pages");

	@Override
	protected void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		context.write(page, one);
		context.progress();
	}

}