package de.measite.wiki.mapreduce.linkgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import de.measite.wiki.mapreduce.io.LinkWritable;
import de.measite.wiki.mapreduce.io.PageInvertWritable;

public class LinkGraphNormalize {

	public static class LinkScoreExtract extends
	Mapper<Text, LinkWritable, Text, DoubleWritable> {

		private final static Text SCORE = new Text("score");

		@Override
		protected void map(Text key, LinkWritable value, Context context)
		throws IOException, InterruptedException {
			context.write(SCORE, new DoubleWritable(value.getScore()));
		}

	}

	public static class MaxDoubleValue extends
	Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values,
		Context context) throws IOException, InterruptedException {
			double max = -1d;
			Iterator<DoubleWritable> iter = values.iterator();
			while (iter.hasNext()) {
				double value = iter.next().get();
				if (value > max) {
					max = value;
				}
			}
			context.write(key, new DoubleWritable(max));
		}

	}

}
