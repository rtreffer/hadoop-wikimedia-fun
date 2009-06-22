package de.measite.wiki.mapreduce.usermap;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import de.measite.wiki.mapreduce.io.LinkWritable;
import de.measite.wiki.mapreduce.io.PageInvertWritable;

public class PageRelationByUser {

	// Step 1, create a simple graph

	public static class MapFunction1 extends
	Mapper<Text, PageInvertWritable, Text, PageInvertWritable> {

		@Override
		protected void map(Text key, PageInvertWritable value, Context context)
		throws IOException, InterruptedException {
			if (key.getBytes()[0] == 'U') {
				super.map(new Text(new String(key.toString()).substring(1)),
				value, context);
			}
		}

	}

	public static class ReduceFunction1 extends
	Reducer<Text, PageInvertWritable, Text, LinkWritable> {

		@Override
		protected void reduce(Text key, Iterable<PageInvertWritable> values,
		Context context) throws IOException, InterruptedException {
			long limit = context.getConfiguration().getLong(
			"scoreinverter.userinvert.maxelements", 1000);
			String username = key.toString();

			Iterator<PageInvertWritable> iterator = values.iterator();
			TreeMap<Long, PageInvertWritable> vals = new TreeMap<Long, PageInvertWritable>();
			double maxScore = Double.MIN_VALUE;
			while (limit > 0 && iterator.hasNext()) {
				PageInvertWritable value = iterator.next();
				long time = value.getTimestamp();
				while (vals.containsKey(time)) {
					time++;
				}
				if (value.getScore() > maxScore) {
					maxScore = value.getScore();
				}
				vals.put(time, new PageInvertWritable(value));
				limit--;
			}
			if (limit == 0) {
				return;
			}

			maxScore = 1d / maxScore / Math.log1p(vals.size());

			while (vals.size() > 1) {
				long t1 = vals.firstKey();
				PageInvertWritable e1 = vals.remove(t1);
				Iterator<Long> iter = vals.keySet().iterator();
				boolean cont = true;
				while (cont && iter.hasNext()) {
					long t2 = iter.next();
					double distance = timeDistance(t1, t2);
					if (distance > 0d) {

						PageInvertWritable e2 = vals.get(t2);

						int compare = e1.getSource().compareTo(e2.getSource());
						if (compare != 0) {

							double score = Math.sqrt(e1.getScore()
							* e2.getScore())
							* distance * maxScore;

							LinkWritable link = null;
							if (compare < 0) {
								link = new LinkWritable(e1.getSource(), e2.getSource(), score);
							} else {
								link = new LinkWritable(e2.getSource(), e1.getSource(), score);
							}
							context.write(new Text(username + "|" + link.getPrimaryKey()), link);

						}
					} else {
						cont = false;
					}
				}
			}
		}

	}

	private final static double timeDistance(long t1, long t2) {
		double delta = Math.abs(t1 - t2) / (30 * 24 * 60 * 60 * 1000);
		return Math.pow(Math.max(1d, 2d - delta), 4d) - 1d;
	}

	// Step 2, merge graph links by user (logarithmic)

	public static class ReduceFunction2 extends
	Reducer<Text, LinkWritable, Text, LinkWritable> {

		@Override
		protected void reduce(Text key, Iterable<LinkWritable> values,
		Context context) throws IOException, InterruptedException {
			Iterator<LinkWritable> iter = values.iterator();
			LinkWritable result = null;
			double score = 0d;
			while (iter.hasNext()) {
				score += (result = iter.next()).getScore();
			}
			score = 1d + Math.log1p(score);
			result.setScore(score);
			key.set(result.getPrimaryKey());
			context.write(key, result);
		}

	}

	// Step 3, merge graph links

	public static class ReduceFunction3 extends
	Reducer<Text, LinkWritable, Text, LinkWritable> {

		@Override
		protected void reduce(Text key, Iterable<LinkWritable> values,
		Context context) throws IOException, InterruptedException {
			Iterator<LinkWritable> iter = values.iterator();
			LinkWritable result = null;
			double score = 0d;
			while (iter.hasNext()) {
				score += (result = iter.next()).getScore();
			}
			score = 1d + Math.log1p(score);
			result.setScore(score);
			context.write(key, result);
		}

	}

	// Combiner for step 2/3

	public static class CombinerFunction23 extends
	Reducer<Text, LinkWritable, Text, LinkWritable> {

		@Override
		protected void reduce(Text key, Iterable<LinkWritable> values,
		Context context) throws IOException, InterruptedException {
			Iterator<LinkWritable> iter = values.iterator();
			LinkWritable result = null;
			double score = 0d;
			while (iter.hasNext()) {
				score += (result = iter.next()).getScore();
			}
			result.setScore(score);
			context.write(key, result);
		}

	}
}
