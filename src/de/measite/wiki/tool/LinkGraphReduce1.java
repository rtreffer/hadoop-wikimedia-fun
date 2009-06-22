package de.measite.wiki.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.measite.wiki.mapreduce.io.LinkWritable;
import de.measite.wiki.mapreduce.linkgraph.LinkGraphReduce;

public class LinkGraphReduce1 extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new LinkGraphReduce1(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 3) {
			System.err.println("Usage: linkgraphreduce1 <infile> <out> <#outfiles>");
			System.exit(2);
		}
		try {
			Job job = new Job(conf, "linkgraphreduce1");
			job.setJarByClass(LinkGraphReduce1.class);
			job.setMapperClass(LinkGraphReduce.MapFunction1.class);
			job.setReducerClass(LinkGraphReduce.ReduceFunction1.class);
			job.setMapOutputValueClass(LinkWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LinkWritable.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.setInputPaths(job, args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileInputFormat.setMinInputSplitSize(job, 128*1024*1024);
			job.setNumReduceTasks(Integer.parseInt(args[2]));
			job.submit();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(3);
		}
		return 0;
	}

}
