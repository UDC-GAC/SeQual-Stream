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
package com.roi.galegot.sequal.sequalmodel.stat.stat;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.stat.Count;
import com.roi.galegot.sequal.sequalmodel.stat.MeanLength;
import com.roi.galegot.sequal.sequalmodel.stat.MeanQuality;
import com.roi.galegot.sequal.sequalmodel.stat.Stat;

public class StatsTest {
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
	public void statCount() {

		/*
		 * Sequence 1
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Sequence 2
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Sequence 3
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original1 = jsc.parallelize(Arrays.asList(seq1));
		JavaRDD<Sequence> original2 = jsc.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> original3 = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));

		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		Double result;
		Stat stat = new Count();

		// Test for empty RDD
		result = stat.measure(emptyRdd);
		assertTrue(result == 0);

		result = stat.measure(original1);
		assertTrue(result == 1);

		result = stat.measure(original2);
		assertTrue(result == 2);

		result = stat.measure(original3);
		assertTrue(result == 3);
	}

	@Test
	public void statCountPair() {

		/*
		 * Sequence 1
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Sequence 2
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Sequence 3
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original1 = jsc.parallelize(Arrays.asList(seq1));
		JavaRDD<Sequence> original2 = jsc.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> original3 = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));

		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		Double result;
		Stat stat = new Count();

		// Test for empty RDD
		result = stat.measurePair(emptyRdd);
		assertTrue(result == 0);

		result = stat.measurePair(original1);
		assertTrue(result == 1);

		result = stat.measurePair(original2);
		assertTrue(result == 2);

		result = stat.measurePair(original3);
		assertTrue(result == 3);
	}

	@Test
	public void statMeanLength() {
		/*
		 * Sequence 1 Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Sequence 2 Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Sequence 3 Length = 28
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original1 = jsc.parallelize(Arrays.asList(seq1));
		JavaRDD<Sequence> original2 = jsc.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> original3 = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));

		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		Double result;
		Stat stat = new MeanLength();

		// Test for empty RDD
		result = stat.measure(emptyRdd);
		assertTrue(result == 0);

		result = stat.measure(original1);
		assertTrue(result == 30);

		result = stat.measure(original2);
		assertTrue(result == 29.5);

		result = stat.measure(original3);
		assertTrue(result == 29);
	}

	@Test
	public void statMeanLengthPair() {
		/*
		 * Sequence 1 Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA_1";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@cluster_8:UMI_CTTTGA_1";
		String seq1s2Pair = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4Pair = "1/04.72,(003,-2-22+00-12./.-.4";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		/*
		 * Sequence 2 Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA_1";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@cluster_12:UMI_GGTCAA_2";
		String seq2s2Pair = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4Pair = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		/*
		 * Sequence 3 Length = 28
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA_1";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1Pair = "@cluster_21:UMI_AGAACA_1";
		String seq3s2Pair = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4Pair = ">=2.660/?:36AD;0<14703640334";
		seq3.setPairSequence(seq3s1Pair, seq3s2Pair, commLine, seq3s4Pair);

		JavaRDD<Sequence> original1 = jsc.parallelize(Arrays.asList(seq1));
		JavaRDD<Sequence> original2 = jsc.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> original3 = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));

		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		Double result;
		Stat stat = new MeanLength();

		// Test for empty RDD
		result = stat.measurePair(emptyRdd);
		assertTrue(result == 0);

		result = stat.measurePair(original1);
		assertTrue(result == 30);

		result = stat.measurePair(original2);
		assertTrue(result == 29.5);

		result = stat.measurePair(original3);
		assertTrue(result == 29);
	}

	@Test
	public void statMeanQuality() {
		/*
		 * Sequence 1 Quality = 14.566666666666666
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Sequence 2 Quality = 30.103448275862068
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Sequence 3 Quality = 20.964285714285715
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		JavaRDD<Sequence> original1 = jsc.parallelize(Arrays.asList(seq1));
		JavaRDD<Sequence> original2 = jsc.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> original3 = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));

		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		Double result;
		Stat stat = new MeanQuality();

		// Test for empty RDD
		result = stat.measure(emptyRdd);
		assertTrue(result == 0);

		result = stat.measure(original1);
		assertTrue(result == 14.57);

		result = stat.measure(original2);
		assertTrue(result == 22.34);

		result = stat.measure(original3);
		assertTrue(result == 21.88);
	}

	@Test
	public void statMeanQualityPair() {
		/*
		 * Sequence 1 Quality = 14.566666666666666
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA_1";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@cluster_8:UMI_CTTTGA_2";
		String seq1s2Pair = "TATCCUNGCAATANTCTCCGAACNGGAGAG";
		String seq1s4Pair = "1/04.72,(003,-2-22+00-12./.-.4";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		/*
		 * Sequence 2 Quality = 30.103448275862068
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA_1";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@cluster_12:UMI_GGTCAA_2";
		String seq2s2Pair = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4Pair = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		/*
		 * Sequence 3 Quality = 20.964285714285715
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA_1";
		String seq3s2 = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1Pair = "@cluster_21:UMI_AGAACA_2";
		String seq3s2Pair = "GGCATTGCAAAATTTNTTSCACCCCCAG";
		String seq3s4Pair = ">=2.660/?:36AD;0<14703640334";
		seq3.setPairSequence(seq3s1Pair, seq3s2Pair, commLine, seq3s4Pair);

		JavaRDD<Sequence> original1 = jsc.parallelize(Arrays.asList(seq1));
		JavaRDD<Sequence> original2 = jsc.parallelize(Arrays.asList(seq1, seq2));
		JavaRDD<Sequence> original3 = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));

		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		Double result;
		Stat stat = new MeanQuality();

		// Test for empty RDD
		result = stat.measurePair(emptyRdd);
		assertTrue(result == 0);

		result = stat.measurePair(original1);
		assertTrue(result == 14.57);

		result = stat.measurePair(original2);
		assertTrue(result == 22.34);

		result = stat.measurePair(original3);
		assertTrue(result == 21.88);
	}
}