package de.measite.wiki.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageInvertWriteable implements Writable {

	private String source;
	private double score;
	private long timestamp;

	public PageInvertWriteable() {
		timestamp = -1L;
	}

	public PageInvertWriteable(String source, double score) {
		this(source,score, -1L);
	}

	public PageInvertWriteable(String source, double score, long timestamp) {
		this.source = source;
		this.score = score;
		this.timestamp = timestamp;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public boolean isUserPage() {
		return source.startsWith("[[User:");
	}

	public boolean isPage() {
		return source.startsWith("[[");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		source = in.readUTF();
		score = in.readDouble();
		timestamp = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(source);
		out.writeDouble(score);
		out.writeLong(timestamp);
	}

}
