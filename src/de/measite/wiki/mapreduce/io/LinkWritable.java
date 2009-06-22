package de.measite.wiki.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LinkWritable implements Writable {

	private String source;
	private String target;
	private double score;

	public LinkWritable(String source, String target, double score) {
		this.source = source;
		this.target = target;
		this.score = score;
	}

	public LinkWritable() {
	}

	public LinkWritable(LinkWritable other) {
		this(other.source, other.target, other.score);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		source = in.readUTF();
		target = in.readUTF();
		score = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(source);
		out.writeUTF(target);
		out.writeDouble(score);
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getPrimaryKey() {
		if (source.compareTo(target) < 0) {
			return source + '|' + target;
		} else {
			return target + '|' + source;
		}
	}
}
