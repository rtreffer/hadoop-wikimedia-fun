package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.XMLInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Very simple M/R to count the pages in a wikimedia xml dump. Used for
 * verification of the xml record reader.
 */
public class PageCount {

	public static class PageMapper extends Mapper<Object, Text, Text, LongWritable> {

		private final static LongWritable one = new LongWritable(1l);
		private final static Text page = new Text("pages");

		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			context.write(page, one);
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 1) {
			System.err.println("Usage: pagecount <infile> <out>");
			System.exit(2);
		}
		try {
			Job job = new Job(conf, "page count");
			job.setJarByClass(PageCount.class);
			job.setMapperClass(PageMapper.class);
			job.setCombinerClass(LongSumReducer.class);
			job.setReducerClass(LongSumReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(3);
		}
	}

}
