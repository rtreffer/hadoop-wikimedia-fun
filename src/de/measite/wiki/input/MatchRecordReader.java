package de.measite.wiki.input;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitEnabledCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MatchRecordReader extends RecordReader<LongWritable, Text> {

	private long splitStart;
	private long splitEnd;
	private long lastStartPos;
	private long lastStartId;
	private long maxRecordSize;
	protected byte[] startSequence;
	protected byte[] endSequence;
	private Seekable fileIn;
	private InputStream dataIn;
	private LongWritable currentKey;
	private Text currentValue;

	protected volatile ByteArrayOutputStream valueBuffer;
	protected volatile OutputStream out;

	@Override
	public void close() throws IOException {
		if (fileIn == null) {
			return;
		}
		dataIn.close();
		fileIn = null;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
	InterruptedException {
		return currentKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return currentValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (fileIn == null) {
			return 1f;
		}
		long splitPos = fileIn.getPos();
		if (splitPos >= splitEnd) {
			return 1f;
		}
		float progress = ((float)(splitPos - splitStart)) / ((float)(splitEnd - splitStart));
		return Math.min(0.99f, progress);
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext attempt)
	throws IOException, InterruptedException {
		currentKey = new LongWritable();
		currentValue = new Text();
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();
		final Configuration conf = attempt.getConfiguration();
		startSequence = conf.get("mapred.matchreader.record.start").getBytes();
		endSequence = conf.get("mapred.matchreader.record.end").getBytes();
		maxRecordSize = conf.getLong("mapred.matchreader.record.maxSize", Long.MAX_VALUE);
		final CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
		conf);
		final FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(split.getPath());
		fileIn.seek(split.getStart());
		splitStart = fileIn.getPos();
		splitEnd = splitStart + split.getLength();
		final CompressionCodec codec = compressionCodecs.getCodec(file);
		if (codec == null) {
			dataIn = fileIn;
			this.fileIn = fileIn;
		} else if (codec instanceof SplitEnabledCompressionCodec) {
			SplitEnabledCompressionCodec splitCodec = (SplitEnabledCompressionCodec) codec;
			dataIn = splitCodec.createInputStream(fileIn, SplitEnabledCompressionCodec.READ_MODE.BYBLOCK);
			this.fileIn = (Seekable) dataIn;
		} else {
			// Classic compression codec, no spits??
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		currentValue.set("");

		if (fileIn == null || fileIn.getPos() > splitEnd) {
			return false;
		}

		// Step 1, seek to the next startSequence

		final byte buf[] = new byte[1];
		boolean start = false;
		int read = dataIn.read(buf);
		if (read == -1) {
			close();
			currentKey = null;
			currentValue = null;
			return false;
		}
		int pos = 0;
		do {
			if (buf[0] == startSequence[pos]) {
				pos ++;
			} else {
				pos = 0;
			}
			if (pos == startSequence.length) {
				start = true;
			} else {
				if (read > 0) {
					if (fileIn.getPos() >= splitEnd) {
						currentKey = null;
						currentValue = null;
						return false;
					}
				}
				read = dataIn.read(buf);
				if (read == -1) {
					close();
					currentKey = null;
					currentValue = null;
					return false;
				}
			}
		} while (!start);

		long startPos = fileIn.getPos();

		startRecord();
		final OutputStream iout = out;
		iout.write(startSequence);

		// Step 2, seek to the end sequence and write to the output buffer
		boolean end = false;
		read = dataIn.read(buf);
		if (read == -1) {
			endRecord();
			close();
			currentKey = null;
			currentValue = null;
			return false;
		}
		iout.write(buf);
		pos = 0;
		do {
			if (buf[0] == endSequence[pos]) {
				pos ++;
			} else {
				pos = 0;
			}
			if (pos == endSequence.length) {
				end = true;
			} else {
				if (valueBuffer.size() > maxRecordSize) {
					endRecord();
					// We simply abort reading and seek the next page start, if possible.
					return nextKeyValue();
				}
				read = dataIn.read(buf);
				if (read == -1) {
					close();
					endRecord();
					currentKey = null;
					currentValue = null;
					return false;
				}
				iout.write(buf);
			}
		} while (!end);

		if (fileIn.getPos() >= splitEnd) {
			// We know that there are no new entries
			close();
		}

		out.flush();

		// Step 3, compute a key / value pair
		byte[] bytes = valueBuffer.toByteArray();
		endRecord();
		currentValue.set(bytes);
		bytes = null;

		if (startPos != lastStartPos) {
			lastStartPos = startPos;
			lastStartId = -1;
		}
		lastStartId++;
		currentKey.set(startPos * 60 + lastStartId);

		return true;
	}

	public void startRecord() throws IOException {
		valueBuffer = new ByteArrayOutputStream();
		out = valueBuffer;
	}

	public void endRecord() {
		valueBuffer = null;
		out = null;
	}

}
