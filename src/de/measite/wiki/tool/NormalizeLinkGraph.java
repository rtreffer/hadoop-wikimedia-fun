package de.measite.wiki.tool;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.measite.wiki.mapreduce.io.LinkWritable;
import de.measite.wiki.mapreduce.linkgraph.LinkGraphAnalyse;
import de.measite.wiki.mapreduce.linkgraph.LinkGraphNormalize;
import de.measite.wiki.mapreduce.linkgraph.LinkGraphAnalyse.LinkScoreExtract;
import de.measite.wiki.mapreduce.linkgraph.LinkGraphAnalyse.MaxDoubleValue;

public class NormalizeLinkGraph extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new NormalizeLinkGraph(), args);
		System.exit(result);
	}

	public Job getMaxScoreJob(Path src, Path dst, Configuration conf) throws IOException {
		Job job = new Job(conf, "normalize:maxlinkscore");
		job.setJarByClass(NormalizeLinkGraph.class);
		job.setMapperClass(LinkScoreExtract.class);
		job.setCombinerClass(MaxDoubleValue.class);
		job.setReducerClass(MaxDoubleValue.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.setInputPaths(job, src);
		FileOutputFormat.setOutputPath(job, dst);
		return job;
	}

	public Job getAnalyzeJob(Path src, Path dst, Path tmp, int iteration, Configuration conf) throws IOException {
		Job job = new Job(conf, "normalize:bucketize(" + (iteration+1) + "/5)");
		job.setJarByClass(NormalizeLinkGraph.class);
		job.setMapperClass(LinkGraphAnalyse.BucketizeMap.class);
		job.setCombinerClass(LongSumReducer.class);
		job.setReducerClass(LongSumReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		if (iteration == 0) {
			FileInputFormat.setInputPaths(job, src);
		} else {
			FileInputFormat.setInputPaths(job, new Path(dst, "i" + iteration));
		}
		FileOutputFormat.setOutputPath(job, tmp);
		return job;
	}

	public Job getNormalizeJob(Path src, Path dst, int iteration, Configuration conf) throws IOException {
		Job job = new Job(conf, "normalize:normalize(" + (iteration+1) + "/5)");
		job.setJarByClass(NormalizeLinkGraph.class);
		job.setMapperClass(LinkGraphNormalize.NormalizeMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LinkWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LinkWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		if (iteration == 0) {
			FileInputFormat.setInputPaths(job, src);
			FileOutputFormat.setOutputPath(job, new Path(dst, "i1"));
		} else {
			FileInputFormat.setInputPaths(job, new Path(dst, "i" + iteration));
			FileOutputFormat.setOutputPath(job, new Path(dst, "i" + (iteration + 1)));
		}
		return job;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		if (args.length != 3) {
			System.err.println("Usage: normalize <in> <out> <#files>");
			return 1;
		}
		FileSystem fs = FileSystem.get(conf);
		Path src = new Path(args[0]);
		Path dst = new Path(args[1]);
		Path dstTmp = new Path(dst, "tmp");
		Path dstFile = new Path(dstTmp, "part-r-00000");
		try {
			Job job = getMaxScoreJob(new Path(args[0]), dstTmp, conf);
			System.out.println("Compute graph maximum");
			job.submit();
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
			return 2;
		}
		System.out.println("Reading result from " + dstFile);
		LineNumberReader lnr = new LineNumberReader(new InputStreamReader(fs.open(dstFile)));
		String line = lnr.readLine();
		lnr.close();
		line = line.substring(line.indexOf("\t") + 1);
		double max = Double.parseDouble(line);
		System.out.println("Maximum: " + line);
		double buckets[] = new double[1000];
		for (int i = 0; i < 1000; i++) {
			buckets[i] = (i + 1) * 0.001 * max;
		}
		for (int i = 0; i < 2; i++) {
			fs.delete(dstTmp, true);

			System.out.println("Bucketize " + i);
			String s[] = new String[buckets.length];
			for (int j = 0; j < 1000; j++) {
				buckets[j] = (j + 1) * 0.001 * max;
			}
			for (int j = 0; j < buckets.length; j++) {
				s[j] = Double.toString(buckets[j]);
			}
			conf.setStrings("linkgraph.analyse.bucketize.buckets", s);
			Job job = getAnalyzeJob(src, dst, dstTmp, i, conf);
			job.submit();
			job.waitForCompletion(true);

			int count[] = new int[buckets.length];
			int total = 0;
			lnr = new LineNumberReader(new InputStreamReader(fs.open(dstFile)));
			line = lnr.readLine();
			while (line != null) {
				String[] split = line.split("\t");
				int index = Integer.parseInt(split[0]);
				int c = Integer.parseInt(split[1]);
				count[index] = c;
				line = lnr.readLine();
			}
			lnr.close();
			for (int j: count) {
				total += j;
			}
			{
				System.out.println("\nHistogram:");
				int linemax = 0;
				int histogram[] = new int[40];
				for (int j = 0; j < 40; j++) {
					for (int k = 0; k < 25; k++) {
						histogram[j] += count[j*25 + k];
					}
					if (histogram[j] >= linemax) {
						linemax = histogram[j] + 1;
					}
				}
				for (int j = 0; j < 25; j++) {
					int display = (histogram[j] * 61) / linemax;
					for (int k = 0; k < display; k++) {
						System.out.print('*');
					}
					System.out.println();
				}
				System.out.println("------------------------------------------------------------");
			}

			double nbuckets[] = new double[buckets.length];
			nbuckets[buckets.length - 1] = buckets[buckets.length - 1];
			int pos = -1;
			int c = 0;
			for (int j = 0; j < buckets.length - 1; j++) {
				double cneed = total * (j + 1d) / buckets.length - 0.001d;
				while (c < cneed) {
					c += count[++pos];
				}
				c -= count[pos--];
				double p = (cneed - c) / count[pos+1];
				if (pos == -1) {
					nbuckets[j] = p * buckets[0];
				} else {
					nbuckets[j] = (1d - p) * buckets[pos] + p * buckets[pos + 1];
				}
			}
			buckets = nbuckets;

			// Normalize
			System.out.println("Normalize");
			for (int j = 0; j < buckets.length; j++) {
				s[j] = Double.toString(buckets[j]);
			}
			conf.setStrings("linkgraph.analyse.bucketize.buckets", s);
			job = getNormalizeJob(src, dst, i, conf);
			job.setNumReduceTasks(Integer.parseInt(args[2]));
			job.submit();
			job.waitForCompletion(true);

			max = 1d;
		}
		fs.delete(dstTmp, true);

		return 0;
	}

}
