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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.measite.wiki.input.WikimediaSimplifyInputFormat;
import de.measite.wiki.mapreduce.PageToTitleMapper;

/**
 * M/R to split the xml into "simplified" pieces. Used to verify the xml output.
 */
public class PageSplit extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PageSplit(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 2) {
			System.err.println("Usage: pagecount <infile> <out>");
			System.exit(2);
		}
		try {
			Job job = new Job(conf, "page count");
			job.setJarByClass(PageSplit.class);
			job.setMapperClass(PageToTitleMapper.class);
			job.setNumReduceTasks(0);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setInputFormatClass(WikimediaSimplifyInputFormat.class);
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
