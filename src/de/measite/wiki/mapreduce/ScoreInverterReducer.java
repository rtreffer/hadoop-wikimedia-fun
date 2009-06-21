package de.measite.wiki.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreInverterReducer extends
Reducer<Text, MapWritable, Text, MapWritable> {

	public final static Text TIMESTAMP = new Text("T");
	public final static Text SOURCE = new Text("F");
	public final static Text SCORE = new Text("S");

	private static class Entry {
		public double score;
		public String source;
		@SuppressWarnings("unused")
		public Entry() {}
		public Entry(double score, String source) {
			super();
			this.score = score;
			this.source = source;
		}
	}

	@Override
	protected void reduce(Text key, Iterable<MapWritable> values,
	Context context) throws IOException, InterruptedException {
		String k = key.toString();
		if (k.startsWith("[[User:")) {
			userReduce(key, values, context);
			return;
		}
		if (k.startsWith("[[")) {
			linkReduce(key, values, context);
			return;
		}
		System.err.println("Can't map " + key.toString());
	}

	protected void linkReduce(Text key, Iterable<MapWritable> values,
	ReduceContext<Text, MapWritable, Text, MapWritable> context)
	throws IOException, InterruptedException {
		long limit = context.getConfiguration().getLong(
		"scoreinverter.userinvert.maxelements", Long.MAX_VALUE);
	}

	protected void userReduce(Text key, Iterable<MapWritable> values,
	ReduceContext<Text, MapWritable, Text, MapWritable> context)
	throws IOException, InterruptedException {
		long limit = context.getConfiguration().getLong(
		"scoreinverter.userinvert.maxelements", Long.MAX_VALUE);
		Iterator<MapWritable> iterator = values.iterator();
		TreeMap<Long, Entry> vals = new TreeMap<Long, Entry>();
		double maxScore = Double.MIN_VALUE;
		while (limit > 0 && iterator.hasNext()) {
			MapWritable value = iterator.next();
			long time = ((LongWritable) value.get(TIMESTAMP)).get();
			double score = ((DoubleWritable) value.get(SCORE)).get();
			while (vals.containsKey(time)) {
				time++;
			}
			if (score > maxScore) {
				maxScore = score;
			}
			String source = ((Text) value.get(SOURCE)).toString();
			vals.put(time, new Entry(score, source));
			limit--;
		}
		if (limit == 0) {
			return;
		}

		maxScore = 1d / maxScore;

		while (vals.size() > 0) {
			long t1 = vals.firstKey();
			Entry e1 = vals.remove(t1);
			Iterator<Long> iter = vals.keySet().iterator();
			boolean cont = true;
			while (cont && iter.hasNext()) {
				long t2 = iter.next();
				double distance = timeDistance(t1, t2);
				if (distance > 0d) {

					Entry e2 = vals.get(t2);

					int compare = e1.source.compareTo(e2.source);
					if (compare != 0) {

						MapWritable outValue = new MapWritable();

						double score = Math.sqrt(e1.score * e2.score) * distance * maxScore;

						outValue.put(SCORE, new DoubleWritable(score));
						outValue.put(SOURCE, key);

						context.write(new Text(e1.source + e2.source), outValue);
						context.write(new Text(e2.source + e1.source), outValue);
					}
				} else {
					cont = false;
				}
			}
		}
	}

	private double timeDistance(long t1, long t2) {
		double delta = Math.abs(t1 - t2) / (30 * 24 * 60 * 60 * 1000);
		return Math.max(0d, 1 + delta * Math.log10(1 - 0.9 * delta));
	}

}
