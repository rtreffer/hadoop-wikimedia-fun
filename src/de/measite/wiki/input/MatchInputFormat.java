package de.measite.wiki.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MatchInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
	TaskAttemptContext context) throws IOException, InterruptedException {
		RecordReader<LongWritable, Text> reader = new MatchRecordReader();
		reader.initialize(split, context);
		return reader;
	}

	@Override
	protected boolean isSplitable(JobContext job, Path path) {
		return true;
	}

}
