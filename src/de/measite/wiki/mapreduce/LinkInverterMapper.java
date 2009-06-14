package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LinkInverterMapper extends
Mapper<Text, MapWritable, Text, MapWritable> {

	@Override
	protected void map(Text key, MapWritable value, Context context) throws IOException,
	InterruptedException {
		String k = new String(key.getBytes());
		if (k.startsWith("[[User:")) {
			return;
		}
		if (!k.startsWith("[[")) {
			return;
		}
		// Huston, we've got a link :) Duplicate it and write out
	}

	
}
