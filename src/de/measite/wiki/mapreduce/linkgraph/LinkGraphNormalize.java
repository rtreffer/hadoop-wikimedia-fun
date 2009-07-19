package de.measite.wiki.mapreduce.linkgraph;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.measite.wiki.mapreduce.io.LinkWritable;

public class LinkGraphNormalize {

	public static class NormalizeMap extends
	Mapper<Text, LinkWritable, Text, LinkWritable> {

		private double[] bucket;

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			String buckets[] = context.getConfiguration().getStrings("linkgraph.analyse.bucketize.buckets");
			if (buckets == null) {
				throw new IllegalStateException("Normalize needs a setting linkgraph.analyse.normalize.buckets");
			}
			double bucket[] = new double[buckets.length];
			for (int i = 0; i < buckets.length; i++) {
				bucket[i] = Double.parseDouble(buckets[i]);
			}
			Arrays.sort(bucket);
			this.bucket = bucket;
		}

		@Override
		protected void map(Text key, LinkWritable value, Context context)
		throws IOException, InterruptedException {
			if (value == null) {
				return;
			}
			int pos = Arrays.binarySearch(bucket, value.getScore());
			if (pos < 0) {
				pos = -1 -pos;
			}
			double span = bucket[pos];
			double score = value.getScore();
			if (pos > 0) {
				double b = bucket[pos - 1];
				span -= b;
				score -= b;
			}
			value.setScore((score / span + pos) * (1d / bucket.length));
			context.write(key, value);
		}

	}

}
