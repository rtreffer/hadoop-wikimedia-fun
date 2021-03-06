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
import de.measite.wiki.mapreduce.io.PageInvertWritable;
import de.measite.wiki.mapreduce.wordmap.PageRelationByWords;

/**
 * Very simple M/R to count the pages in a wikimedia xml dump. Used for
 * verification of the xml record reader.
 */
public class WordPageGraphExtract1 extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new WordPageGraphExtract1(), args);
		System.exit(result);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 3) {
			System.err.println("Usage: wordpagegraph <infile> <out> <#outfiles>");
			System.exit(2);
		}
		try {
			Job job = new Job(conf, "wordpagegraph");
			job.setJarByClass(WordPageGraphExtract1.class);
			job.setMapperClass(PageRelationByWords.MapFunction1.class);
			job.setReducerClass(PageRelationByWords.ReduceFunction1.class);
			job.setMapOutputValueClass(PageInvertWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LinkWritable.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.setInputPaths(job, args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			// 1GB Split, NOOP-MAP
			FileInputFormat.setMinInputSplitSize(job, 1024*1024*1024);
			job.setNumReduceTasks(Integer.parseInt(args[2]));
			job.submit();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(3);
		}
		return 0;
	}

}
