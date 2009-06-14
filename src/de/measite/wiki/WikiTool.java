package de.measite.wiki;

import org.apache.hadoop.util.ProgramDriver;

import de.measite.wiki.tool.PageCount;
import de.measite.wiki.tool.PageInvertion;
import de.measite.wiki.tool.PageRelation;
import de.measite.wiki.tool.PageSplit;

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
			pgd.addClass("pageinvert", PageInvertion.class, "invert step for debugging");
			pgd.addClass("pagerelation", PageRelation.class, "full page relation computation");
			exitCode = pgd.driver(args);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
