package de.measite.wiki.mapreduce.linkgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import de.measite.wiki.mapreduce.io.LinkWritable;

/**
 * LinkGraphReduce will reorder all vertices to the edges, sort by score
 * and reunion the best best vertices.
 */
public class LinkGraphReduce {

	public static class MapFunction1 extends
	Mapper<Text, LinkWritable, Text, LinkWritable> {

		@Override
		protected void map(Text key, LinkWritable value, Context context)
		throws IOException, InterruptedException {
			context.write(new Text(value.getSource()), value);
			context.write(new Text(value.getTarget()), value);
		}

	}

	public static class ReduceFunction1 extends
	Reducer<Text, LinkWritable, Text, LinkWritable> {

		@Override
		protected void reduce(Text key, Iterable<LinkWritable> values,
		Context context) throws IOException, InterruptedException {
			long limit = context.getConfiguration().getLong(
			"linkgraph.reduce.keepelements", 100);
			long elements = 0;
			Iterator<LinkWritable> iter = values.iterator();
			TreeMap<Double, ArrayList<LinkWritable>> scores = new TreeMap<Double, ArrayList<LinkWritable>>();
			while (iter.hasNext()) {
				LinkWritable link = new LinkWritable(iter.next());
				double score = link.getScore();
				ArrayList<LinkWritable> list = scores.get(score);
				if (list == null) {
					list = new ArrayList<LinkWritable>(2);
					scores.put(score, list);
				}
				list.add(link);
				elements++;
				if (elements > limit) {
					if (elements - scores.firstEntry().getValue().size() >= limit) {
						scores.remove(scores.firstKey());
					}
				}
			}
			for (ArrayList<LinkWritable> list: scores.values().toArray(new ArrayList[0])) {
				for (LinkWritable l: list) {
					context.write(new Text(l.getSource() + '|' + l.getTarget()), l);
				}
			}
		}

	}

	public static class ReduceFunction2 extends
	Reducer<Text, LinkWritable, Text, LinkWritable> {

		@Override
		protected void reduce(Text key, Iterable<LinkWritable> values,
		Context context) throws IOException, InterruptedException {
			Iterator<LinkWritable> iter = values.iterator();
			if (!iter.hasNext()) {
				return;
			}
			context.write(key, iter.next());
		}

	}

}
