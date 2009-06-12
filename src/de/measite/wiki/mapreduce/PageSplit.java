package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.measite.wiki.input.WikimediaSimplifyInputFormat;

/**
 * M/R to split the xml into "simplified" pieces. Used to verify the xml output.
 */
public class PageSplit extends Configured implements Tool {

	public static class PageSplitMapper extends Mapper<Object, Text, Text, Text> {

		private final Text outKey = new Text();

		@Override
		protected void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			if (value == null) {
				return;
			}
			String text = value.toString();
			int pos = text.indexOf("\n    <title>");
			if (pos == -1) {
				return;
			}
			text = text.substring(pos + 12);
			pos = text.indexOf("</title>");
			outKey.set(text.substring(0, pos));
			context.write(outKey, value);
		}

	}

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
		conf.set("mapred.matchreader.record.start", "\n  <page>");
		conf.set("mapred.matchreader.record.end", "\n  </page>");
		conf.setLong("mapred.matchreader.record.maxSize", 180*1024*1024);
		try {
			Job job = new Job(conf, "page count");
			job.setJarByClass(PageSplit.class);
			job.setMapperClass(PageSplitMapper.class);
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
