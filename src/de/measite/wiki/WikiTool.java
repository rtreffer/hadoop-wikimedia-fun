package de.measite.wiki;

import org.apache.hadoop.util.ProgramDriver;

import de.measite.wiki.tool.LinkGraphReduce1;
import de.measite.wiki.tool.LinkGraphReduce2;
import de.measite.wiki.tool.MaxLinkScore;
import de.measite.wiki.tool.PageCount;
import de.measite.wiki.tool.PageInvertion;
import de.measite.wiki.tool.PageRelation;
import de.measite.wiki.tool.PageSplit;
import de.measite.wiki.tool.UserPageGraphExtract1;
import de.measite.wiki.tool.UserPageGraphExtract2;
import de.measite.wiki.tool.UserPageGraphExtract3;
import de.measite.wiki.tool.WordPageGraphExtract1;
import de.measite.wiki.tool.WordPageGraphExtract2;

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
			pgd.addClass("maxlinkscore", MaxLinkScore.class, "maximum link score computation");
			pgd.addClass("userpagegraph1", UserPageGraphExtract1.class, "extract the page graph after an invert (step1)");
			pgd.addClass("userpagegraph2", UserPageGraphExtract2.class, "extract the page graph after an invert (step2)");
			pgd.addClass("userpagegraph3", UserPageGraphExtract3.class, "extract the page graph after an invert (step3)");
			pgd.addClass("wordpagegraph1", WordPageGraphExtract1.class, "extract the word graph after an invert (step1)");
			pgd.addClass("wordpagegraph2", WordPageGraphExtract2.class, "extract the word graph after an invert (step2)");
			pgd.addClass("linkgraphreduce1", LinkGraphReduce1.class, "simplify the link graph by keeping the 100 best links per edge (step1)");
			pgd.addClass("linkgraphreduce2", LinkGraphReduce2.class, "simplify the link graph by keeping the 100 best links per edge (step2)");
			exitCode = pgd.driver(args);
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
