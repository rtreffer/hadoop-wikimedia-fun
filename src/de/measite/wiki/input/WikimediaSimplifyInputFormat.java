package de.measite.wiki.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class WikimediaSimplifyInputFormat extends MatchInputFormat {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
	TaskAttemptContext context) throws IOException, InterruptedException {
		RecordReader<LongWritable, Text> reader = new WikimediaSimplifyRecordReader();
		reader.initialize(split, context);
		return reader;
	}


}
