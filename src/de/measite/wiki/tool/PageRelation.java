package de.measite.wiki.tool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.measite.wiki.input.WikimediaSimplifyInputFormat;
import de.measite.wiki.mapreduce.PageInvertMapper;
import de.measite.wiki.mapreduce.ScoreInverterReducer;
import de.measite.wiki.mapreduce.io.PageInvertWritable;

/**
 * This is the M/R we've been heading for. InputReader: read/split/simplify the
 * input Map: Invert the content (reverse links, text to page, edits by user)
 * Reduce: Take page lists based on text/user/links and compute pate-to-page
 * scores Map: Sum the page scores and
 */
public class PageRelation extends Configured implements Tool {

	/**
	 * @param args	@Override

	 */
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new PageRelation(),
		args);
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
		conf.setLong("mapred.matchreader.record.maxSize", 180 * 1024 * 1024);
		conf.setLong("scoreinverter.userinvert.maxelements", 100000);
		conf.setLong("pagerelation.scoreinverter.max", 100000);
		try {
			Job job = new Job(conf, "page count");
			job.setJarByClass(PageRelation.class);

			FileInputFormat.setInputPaths(job, args[0]);
			FileInputFormat.setMinInputSplitSize(job, 40 * 1024 * 1024);
			job.setInputFormatClass(WikimediaSimplifyInputFormat.class);

			job.setMapperClass(PageInvertMapper.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(PageInvertWritable.class);

			job.setPartitionerClass(HashPartitioner.class);
			job.setSpeculativeExecution(false);

			job.setReducerClass(ScoreInverterReducer.class);
			job.setNumReduceTasks(1);

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
