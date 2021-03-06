package de.measite.wiki.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.measite.wiki.input.MatchInputFormat;
import de.measite.wiki.mapreduce.PageCountMapper;

/**
 * Very simple M/R to count the pages in a wikimedia xml dump. Used for
 * verification of the xml record reader.
 */
public class PageCount extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PageCount(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: pagecount <infile> <out>");
			System.exit(2);
		}
		conf.set("mapred.matchreader.record.start", "\n  <page>");
		conf.set("mapred.matchreader.record.end", "\n  </page>");
		conf.setLong("mapred.matchreader.record.maxSize", 100*1024*1024);
		try {
			Job job = new Job(conf, "page count");
			job.setJarByClass(PageCount.class);
			job.setMapperClass(PageCountMapper.class);
			job.setCombinerClass(LongSumReducer.class);
			job.setReducerClass(LongSumReducer.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setInputFormatClass(MatchInputFormat.class);
			FileInputFormat.setInputPaths(job, args[0]);
			FileInputFormat.setMinInputSplitSize(job, 40*1024*1024);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.submit();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(3);
		}
		return 0;
	}

}
