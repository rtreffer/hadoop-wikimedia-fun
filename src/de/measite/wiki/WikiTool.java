package de.measite.wiki;

import org.apache.hadoop.util.ProgramDriver;

import de.measite.wiki.mapreduce.PageCount;

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
			exitCode = pgd.driver(args);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
