package de.measite.wiki.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitEnabledCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.eclipse.jdt.internal.core.CreateInitializerOperation;

public class MatchRecordReader extends RecordReader<LongWritable, Text> {

	private long start;
	private long end;
	private byte[] startSequence;
	private byte[] endSequence;

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
	InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext attempt)
	throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		final Configuration conf = attempt.getConfiguration();
		final CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
		conf);
		final FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(split.getPath());
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		if (codec instanceof SplitEnabledCompressionCodec) {
			SplitEnabledCompressionCodec splitCodec = (SplitEnabledCompressionCodec) codec;
			splitCodec.createInputStream(fileIn, SplitEnabledCompressionCodec.READ_MODE.BYBLOCK);
		}
		fileIn.seek(start);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}

}
