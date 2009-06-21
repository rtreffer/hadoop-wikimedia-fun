package de.measite.wiki.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import de.measite.wiki.mapreduce.io.PageInvertWritable;

public class LinkMaxScoreReducer extends
Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private final static Text outKey = new Text("max");

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
	Context context) throws IOException, InterruptedException {
		Iterator<DoubleWritable> iter = values.iterator();
		if (!iter.hasNext()) {
			return;
		}
		double maxScore = Double.MIN_VALUE;
		while (iter.hasNext()) {
			double score = iter.next().get();
			if (score > maxScore) {
				maxScore = score;
			}
		}
		context.write(outKey, new DoubleWritable(maxScore));
	}

	public static class Map extends
	Mapper<Text, PageInvertWritable, Text, DoubleWritable> {

		@Override
		protected void map(Text key, PageInvertWritable value, Context context) throws IOException,
		InterruptedException {
			String k = new String(key.toString());
			if (!k.startsWith("L[[")) {
				return;
			}
			context.write(outKey, new DoubleWritable(value.getScore()));
		}

	}

}
