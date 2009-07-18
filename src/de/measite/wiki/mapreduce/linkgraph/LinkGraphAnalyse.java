package de.measite.wiki.mapreduce.linkgraph;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.measite.wiki.mapreduce.io.LinkWritable;

public class LinkGraphAnalyse {

	public static class BucketizeMap extends
	Mapper<Text, LinkWritable, LongWritable, LongWritable> {

		private double[] bucket;
		private final static LongWritable ONE = new LongWritable(1L);

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			String buckets[] = context.getConfiguration().getStrings("linkgraph.analyse.bucketize.buckets");
			if (buckets == null) {
				throw new IllegalStateException("Bucketize needs a setting linkgraph.analyse.bucketize.buckets");
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
			if (pos >= 0) {
				context.write(new LongWritable(pos), ONE);
			} else {
				context.write(new LongWritable(-pos - 1), ONE);
			}
		}

	}

}
