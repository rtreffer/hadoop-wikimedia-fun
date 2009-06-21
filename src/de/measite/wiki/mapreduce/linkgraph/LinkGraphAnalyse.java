package de.measite.wiki.mapreduce.linkgraph;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.measite.wiki.mapreduce.io.LinkWritable;

public class LinkGraphAnalyse {

	public static class BucketizeMap extends
	Mapper<Text, LinkWritable, DoubleWritable, LongWritable> {

		private double[] bucket;
		private final static LongWritable ONE = new LongWritable(1L);

		@Override
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			String buckets = context.getConfiguration().get("linkgraph.analyse.bucketize.buckets");
			if (buckets == null) {
				throw new IllegalStateException("Bucketize needs a setting linkgraph.analyse.bucketize.buckets");
			}
			String[] split = buckets.split("[, ]");
			double bucket[] = new double[split.length + 1];
			for (int i = 0; i < split.length; i++) {
				bucket[i] = Double.parseDouble(split[i].trim());
			}
			bucket[split.length] = Double.MAX_VALUE;
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
				context.write(new DoubleWritable(bucket[pos]), ONE);
			} else {
				context.write(new DoubleWritable(Math.abs(bucket[pos]) - 1), ONE);
			}
		}

	}

}
