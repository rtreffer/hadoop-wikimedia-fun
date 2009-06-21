package de.measite.wiki.mapreduce.wordmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import de.measite.wiki.mapreduce.io.LinkWritable;
import de.measite.wiki.mapreduce.io.PageInvertWritable;

public class PageRelationByWords {

	public static class MapFunction1 extends
	Mapper<Text, PageInvertWritable, Text, PageInvertWritable> {

		@Override
		protected void map(Text key, PageInvertWritable value, Context context)
		throws IOException, InterruptedException {
			if (key.getBytes()[0] == 'W') {
				super.map(new Text(new String(key.toString()).substring(1)),
				value, context);
			}
		}

	}

	public static class ReduceFunction1 extends
	Reducer<Text, PageInvertWritable, Text, LinkWritable> {

		@Override
		protected void reduce(Text key, Iterable<PageInvertWritable> values,
		Context context) throws IOException, InterruptedException {
			long limit = context.getConfiguration().getLong(
			"scoreinverter.wordinvert.maxelements", 1000);
			ArrayList<PageInvertWritable> inv = new ArrayList<PageInvertWritable>((int)Math.min(10000, limit + 2));
			Iterator<PageInvertWritable> iter = values.iterator();
			while (limit > 0 && iter.hasNext()) {
				inv.add(new PageInvertWritable(iter.next()));
				limit--;
			}
			if (limit < 0) {
				return;
			}
			PageInvertWritable[] array = inv.toArray(new PageInvertWritable[inv.size()]);
			for (int i = 0; i < array.length; i++) {
				final PageInvertWritable e1 = array[i];
				for (int j = i+1; j < array.length; j++) {

					final PageInvertWritable e2 = array[j];

					int compare = e1.getSource().compareTo(e2.getSource());

					double score = e1.getScore() + e2.getScore();

					if (compare < 0) {
						LinkWritable link = new LinkWritable(e1.getSource(), e2.getSource(), score);
						context.write(new Text(e1.getSource() + "|" + e2.getSource()), link);
					} else  if (compare > 0){
						LinkWritable link = new LinkWritable(e2.getSource(), e1.getSource(), score);
						context.write(new Text(e2.getSource() + "|" + e1.getSource()), link);
					}

				}
			}
		}

	}

}
