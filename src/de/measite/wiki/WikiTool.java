package de.measite.wiki;

import org.apache.hadoop.util.ProgramDriver;

import de.measite.wiki.mapreduce.PageCount;
import de.measite.wiki.mapreduce.PageSplit;

public class WikiTool {

	/**
	 * @param args
	 *            command line arguments
	 */
	public static void main(String[] args) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("pagecount", PageCount.class, "checking the number of <page> entries");
			pgd.addClass("pagesplit", PageSplit.class, "split pages to title-named lists");
			exitCode = pgd.driver(args);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
