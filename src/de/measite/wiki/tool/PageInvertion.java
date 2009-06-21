package de.measite.wiki.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.measite.wiki.input.WikimediaSimplifyInputFormat;
import de.measite.wiki.mapreduce.init.PageInvertFastMapper;
import de.measite.wiki.mapreduce.io.PageInvertWritable;

/**
 * This is the M/R that inverts links, pages, and users and writes some big
 * sequence files
 */
public class PageInvertion extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PageInvertion(),
		args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 3) {
			System.err.println("Usage: pageinvert <infile> <out> <#filesout>");
			System.exit(2);
		}
		try {
			Job job = new Job(conf, "pageinvert");
			job.setJarByClass(PageInvertion.class);

			FileInputFormat.setInputPaths(job, args[0]);
			FileInputFormat.setMinInputSplitSize(job, 40 * 1024 * 1024);
			job.setInputFormatClass(WikimediaSimplifyInputFormat.class);

			job.setMapperClass(PageInvertFastMapper.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(PageInvertWritable.class);

			job.setPartitionerClass(HashPartitioner.class);
			job.setSpeculativeExecution(false);

			job.setNumReduceTasks(Integer.parseInt(args[2]));

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(PageInvertWritable.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			job.submit();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(3);
		}
		return 0;
	}

}
