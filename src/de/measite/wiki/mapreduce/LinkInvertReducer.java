package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import de.measite.wiki.mapreduce.io.PageInvertWritable;

public class LinkInvertReducer extends
Reducer<Text, PageInvertWritable, Text, PageInvertWritable> {

	@Override
	protected void reduce(Text key, Iterable<PageInvertWritable> value,
	Context context) throws IOException,
	InterruptedException {
	}

}
