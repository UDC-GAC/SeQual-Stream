/*
 * This file is part of SeQual.
 * 
 * SeQual is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SeQual is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SeQual.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.roi.galegot.sequal.sequalmodel.format.formatter;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.formatter.DNAToRNA;
import com.roi.galegot.sequal.sequalmodel.formatter.FASTQToFASTA;
import com.roi.galegot.sequal.sequalmodel.formatter.Formatter;
import com.roi.galegot.sequal.sequalmodel.formatter.RNAToDNA;

public class FormattersTest {
	private static SparkConf spc;
	private static JavaSparkContext jsc;

	private static String commLine = "+";

	@BeforeClass
	public static void setupSpark() throws IOException {
		spc = new SparkConf().setAppName("SeQual").setMaster("local[*]");
		jsc = new JavaSparkContext(spc);
		jsc.setLogLevel("ERROR");
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void formatDNAToRNA() {
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCTCGCAATACTCTCCGAACAGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTGCAGATCAATATATGCTAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s2 = "UAUCCUCGCAAUACUCUCCGAACAGGAGAG";
		Sequence seq3 = new Sequence(seq1s1, seq3s2, commLine, seq1s4);

		String seq4s2 = "GCAGUUGCAGAUCAAUAUAUGCUAGAGCA";
		Sequence seq4 = new Sequence(seq2s1, seq4s2, commLine, seq2s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2));
		List<Sequence> formatted;
		Formatter formatter = new DNAToRNA();

		formatted = formatter.format(original).collect();
		assertTrue(formatted.contains(seq3));
		assertTrue(formatted.contains(seq4));
	}

	@Test
	public void formatDNAToRNAPair() {
		String seq1s1 = "@cluster_8:UMI_CTTTGA_1";
		String seq1s2 = "TATCCTCGCAATACTCTCCGAACAGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@cluster_8:UMI_CTTTGA_2";
		String seq1s2Pair = "TATCCTCGCAATACTCTCCGAACAGGATTA";
		String seq1s4Pair = "1/04.72,(003,-2-22+00-12./.--.";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@cluster_12:UMI_GGTCAA_1";
		String seq2s2 = "GCAGTTGCAGATCAATATATGCTAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@cluster_12:UMI_GGTCAA_2";
		String seq2s2Pair = "GCAGTTGCAGATCAATATATGCTAGATTT";
		String seq2s4Pair = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq3s2 = "UAUCCUCGCAAUACUCUCCGAACAGGAGAG";
		Sequence seq3 = new Sequence(seq1s1, seq3s2, commLine, seq1s4);

		String seq3s2Pair = "UAUCCUCGCAAUACUCUCCGAACAGGAUUA";
		seq3.setPairSequence(seq1s1Pair, seq3s2Pair, commLine, seq1s4Pair);

		String seq4s2 = "GCAGUUGCAGAUCAAUAUAUGCUAGAGCA";
		Sequence seq4 = new Sequence(seq2s1, seq4s2, commLine, seq2s4);

		String seq4s2Pair = "GCAGUUGCAGAUCAAUAUAUGCUAGAUUU";
		seq4.setPairSequence(seq2s1Pair, seq4s2Pair, commLine, seq2s4Pair);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2));
		List<Sequence> formatted;
		Formatter formatter = new DNAToRNA();

		formatted = formatter.format(original).collect();
		assertTrue(formatted.contains(seq3));
		assertTrue(formatted.contains(seq4));
	}

	@Test
	public void formatRNAToDNA() {
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "UAUCCUCGCAAUACUCUCCGAACAGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGUUGCAGAUCAAUAUAUGCUAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s2 = "TATCCTCGCAATACTCTCCGAACAGGAGAG";
		Sequence seq3 = new Sequence(seq1s1, seq3s2, commLine, seq1s4);

		String seq4s2 = "GCAGTTGCAGATCAATATATGCTAGAGCA";
		Sequence seq4 = new Sequence(seq2s1, seq4s2, commLine, seq2s4);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2));
		List<Sequence> formatted;
		Formatter formatter = new RNAToDNA();

		formatted = formatter.format(original).collect();
		assertTrue(formatted.contains(seq3));
		assertTrue(formatted.contains(seq4));
	}

	@Test
	public void formatRNAToDNAPair() {
		String seq1s1 = "@cluster_8:UMI_CTTTGA_1";
		String seq1s2 = "UAUCCUCGCAAUACUCUCCGAACAGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@cluster_8:UMI_CTTTGA_2";
		String seq1s2Pair = "UAUCCUCGCAAUACUCUCCGAACAGGAUUA";
		String seq1s4Pair = "1/04.72,(003,-2-22+00-12./.-.4";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@cluster_12:UMI_GGTCAA_1";
		String seq2s2 = "GCAGUUGCAGAUCAAUAUAUGCUAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@cluster_12:UMI_GGTCAA_2";
		String seq2s2Pair = "GCAGUUGCAGAUCAAUAUAUGCUAGAUUU";
		String seq2s4Pair = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq3s2 = "TATCCTCGCAATACTCTCCGAACAGGAGAG";
		Sequence seq3 = new Sequence(seq1s1, seq3s2, commLine, seq1s4);

		String seq3s2Pair = "TATCCTCGCAATACTCTCCGAACAGGATTA";
		seq3.setPairSequence(seq1s1Pair, seq3s2Pair, commLine, seq1s4Pair);

		String seq4s2 = "GCAGTTGCAGATCAATATATGCTAGAGCA";
		Sequence seq4 = new Sequence(seq2s1, seq4s2, commLine, seq2s4);

		String seq4s2Pair = "GCAGTTGCAGATCAATATATGCTAGATTT";
		seq4.setPairSequence(seq2s1Pair, seq4s2Pair, commLine, seq2s4Pair);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2));
		List<Sequence> formatted;
		Formatter formatter = new RNAToDNA();

		formatted = formatter.format(original).collect();
		assertTrue(formatted.contains(seq3));
		assertTrue(formatted.contains(seq4));
	}

	@Test
	public void formatFASTQToFASTA() {
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "UAUCCUCGCAAUACUCUCCGAACAGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGUUGCAGAUCAAUAUAUGCUAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA";
		String seq2s1fa = ">cluster_12:UMI_GGTCAA";
		Sequence seq3 = new Sequence(seq1s1fa, seq1s2);
		Sequence seq4 = new Sequence(seq2s1fa, seq2s2);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2));
		List<Sequence> formatted;
		Formatter formatter = new FASTQToFASTA();

		formatted = formatter.format(original).collect();

		assertTrue(formatted.contains(seq3));
		assertTrue(formatted.contains(seq4));
	}

	@Test
	public void formatFASTQToFASTAPair() {
		String seq1s1 = "@cluster_8:UMI_CTTTGA_1";
		String seq1s2 = "UAUCCUCGCAAUACUCUCCGAACAGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@cluster_8:UMI_CTTTGA_2";
		String seq1s2Pair = "UAUCCUCGCAAUACUCUCCGTTCAGGAGCT";
		String seq1s4Pair = "1/04.72,(003,-2-.422+00-12./.-";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGUUGCAGAUCAAUAUAUGCUAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@cluster_12:UMI_GGTCAA";
		String seq2s2Pair = "GCAGUUGCAGAUCAAUAUAUGCUAGAGTT";
		String seq2s4Pair = "?7?AEEC@>=1?A?EEEB9ECB?.A==:B";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq1s1fa = ">cluster_8:UMI_CTTTGA_1";
		String seq1s1faPair = ">cluster_8:UMI_CTTTGA_2";
		String seq2s1fa = ">cluster_12:UMI_GGTCAA_1";
		String seq2s1faPair = ">cluster_12:UMI_GGTCAA_2";

		Sequence seq3 = new Sequence(seq1s1fa, seq1s2);
		seq3.setPairSequence(seq1s1faPair, seq1s2Pair);

		Sequence seq4 = new Sequence(seq2s1fa, seq2s2);
		seq4.setPairSequence(seq2s1faPair, seq2s2Pair);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2));
		List<Sequence> formatted;
		Formatter formatter = new FASTQToFASTA();

		formatted = formatter.format(original).collect();

		assertTrue(formatted.contains(seq3));
		assertTrue(formatted.contains(seq4));
	}
}