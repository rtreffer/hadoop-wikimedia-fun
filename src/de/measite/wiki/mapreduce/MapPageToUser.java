package de.measite.wiki.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapPageToUser extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text arg0, Text arg1, Context arg2) throws IOException,
	InterruptedException {
		super.map(arg0, arg1, arg2);
	}

}
