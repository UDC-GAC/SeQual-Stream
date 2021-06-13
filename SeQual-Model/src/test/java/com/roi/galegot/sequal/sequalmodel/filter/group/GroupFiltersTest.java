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
package com.roi.galegot.sequal.sequalmodel.filter.group;

import static org.junit.Assert.assertEquals;
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
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class GroupFiltersTest {
	private static SparkConf spc;
	private static JavaSparkContext jsc;

	private static String commLine = "+";

	@BeforeClass
	public static void setupSpark() throws IOException {
		spc = new SparkConf().setAppName("SeQual").setMaster("local[1]");
		jsc = new JavaSparkContext(spc);
		jsc.setLogLevel("ERROR");
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void filterDistinct() {
		String seq1s1 = "@Original";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@OriginalCopiaMasCalidad";
		String seq2s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s1 = "@OriginalUnoDistinto";
		String seq3s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq1s1fa = ">Original";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq2s1fa = ">OriginalCopia";
		String seq2s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq3s1fa = ">OriginalUnoDistinto";
		String seq3s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		// seq1 added twice for testing purposes
		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new Distinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2fa) || list.contains(seq1fa));
		assertTrue(list.contains(seq3fa));
	}

	@Test
	public void filterDistinctPair() {
		String seq1s1 = "@Original_1";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@Original_2";
		String seq1s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@OriginalCopiaMasCalidad_1";
		String seq2s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@OriginalCopiaMasCalidad_1";
		String seq2s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq2s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq3s1 = "@OriginalUnoDistinto_1";
		String seq3s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1Pair = "@OriginalUnoDistinto_1";
		String seq3s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		String seq3s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		seq3.setPairSequence(seq3s1Pair, seq3s2Pair, commLine, seq3s4Pair);

		String seq1s1fa = ">Original_1";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq1s1faPair = ">Original_2";
		String seq1s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		seq1fa.setPairSequence(seq1s1faPair, seq1s2faPair);

		String seq2s1fa = ">OriginalCopia_1";
		String seq2s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq2s1faPair = ">OriginalCopia_2";
		String seq2s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		seq2fa.setPairSequence(seq2s1faPair, seq2s2faPair);

		String seq3s1fa = ">OriginalUnoDistinto_1";
		String seq3s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		String seq3s1faPair = ">OriginalUnoDistinto_2";
		String seq3s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		seq3fa.setPairSequence(seq3s1faPair, seq3s2faPair);

		// seq1 added twice for testing purposes
		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new Distinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2fa) || list.contains(seq1fa));
		assertTrue(list.contains(seq3fa));
	}

	@Test
	public void filterAlmostDistinct() {
		String seq1s1 = "@Original";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@OriginalCopiaMasCalidad";
		String seq2s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s1 = "@OriginalUnoDistinto";
		String seq3s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq4s1 = "@OriginalDosDistintos";
		String seq4s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCAA";
		String seq4s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq4 = new Sequence(seq4s1, seq4s2, commLine, seq4s4);

		String seq1s1fa = ">Original";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq2s1fa = ">OriginalCopia";
		String seq2s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq3s1fa = ">OriginalUnoDistinto";
		String seq3s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		String seq4s1fa = ">OriginalDosDistintos";
		String seq4s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCAA";
		Sequence seq4fa = new Sequence(seq4s1fa, seq4s2fa);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3, seq4));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa, seq4fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new AlmostDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		ExecutionParametersManager.setParameter("MaxDifference", "");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq4));

		ExecutionParametersManager.setParameter("MaxDifference", "1");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq4));

		ExecutionParametersManager.setParameter("MaxDifference", "2");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("MaxDifference", "");
		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq1fa) || list.contains(seq2fa));
		assertTrue(list.contains(seq3fa));
		assertTrue(list.contains(seq4fa));

		ExecutionParametersManager.setParameter("MaxDifference", "1");
		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1fa) || list.contains(seq2fa) || list.contains(seq3fa));
		assertTrue(list.contains(seq4fa));

		ExecutionParametersManager.setParameter("MaxDifference", "2");
		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq1fa) || list.contains(seq2fa) || list.contains(seq3fa) || list.contains(seq4fa));
	}

	@Test
	public void filterAlmostDistinctPair() {
		String seq1s1 = "@Original_1";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@Original_2";
		String seq1s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@OriginalCopiaMasCalidad_1";
		String seq2s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@OriginalCopiaMasCalidad_2";
		String seq2s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq2s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq3s1 = "@OriginalUnoDistinto_1";
		String seq3s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1Pair = "@OriginalUnoDistinto_2";
		String seq3s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		String seq3s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq3.setPairSequence(seq3s1Pair, seq3s2Pair, commLine, seq3s4Pair);

		String seq4s1 = "@OriginalDosDistintos_1";
		String seq4s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCAA";
		String seq4s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq4 = new Sequence(seq4s1, seq4s2, commLine, seq4s4);

		String seq4s1Pair = "@OriginalDosDistintos_2";
		String seq4s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCAA";
		String seq4s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq4.setPairSequence(seq4s1Pair, seq4s2Pair, commLine, seq4s4Pair);

		String seq1s1fa = ">Original_1";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq1s1faPair = ">Original_2";
		String seq1s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		seq1fa.setPairSequence(seq1s1faPair, seq1s2faPair);

		String seq2s1fa = ">OriginalCopia_1";
		String seq2s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq2s1faPair = ">OriginalCopia_2";
		String seq2s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		seq2fa.setPairSequence(seq2s1faPair, seq2s2faPair);

		String seq3s1fa = ">OriginalUnoDistinto_1";
		String seq3s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		String seq3s1faPair = ">OriginalUnoDistinto_2";
		String seq3s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCA";
		seq3fa.setPairSequence(seq3s1faPair, seq3s2faPair);

		String seq4s1fa = ">OriginalDosDistintos_1";
		String seq4s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCAA";
		Sequence seq4fa = new Sequence(seq4s1fa, seq4s2fa);

		String seq4s1faPair = ">OriginalDosDistintos_2";
		String seq4s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCAA";
		seq4fa.setPairSequence(seq4s1faPair, seq4s2faPair);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3, seq4));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa, seq4fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new AlmostDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		ExecutionParametersManager.setParameter("MaxDifference", "");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq4));

		ExecutionParametersManager.setParameter("MaxDifference", "1");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq4));

		ExecutionParametersManager.setParameter("MaxDifference", "2");
		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("MaxDifference", "");
		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 3);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 3);
		assertTrue(list.contains(seq1fa) || list.contains(seq2fa));
		assertTrue(list.contains(seq3fa));
		assertTrue(list.contains(seq4fa));

		ExecutionParametersManager.setParameter("MaxDifference", "1");
		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 2);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 2);
		assertTrue(list.contains(seq1fa) || list.contains(seq2fa) || list.contains(seq3fa));
		assertTrue(list.contains(seq4fa));

		ExecutionParametersManager.setParameter("MaxDifference", "2");
		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq1fa) || list.contains(seq2fa) || list.contains(seq3fa) || list.contains(seq4fa));
	}

	@Test
	public void filterReverseDistinct() {
		String seq1s1 = "@Original";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@ReverseCalidadIgual";
		String seq2s2 = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s1 = "@ReverseMasCalidad";
		String seq3s2 = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq1s1fa = ">Original";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq2s1fa = ">Reverse";
		String seq2s2fa = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq3s1fa = ">ReverseCopia";
		String seq3s2fa = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new ReverseDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);

		list = new ArrayList<>(filtered.collect());

		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
	}

	@Test
	public void filterReverseDistinctPair() {
		String seq1s1 = "@Original_1";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@Original_2";
		String seq1s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@ReverseCalidadIgual_1";
		String seq2s2 = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@ReverseCalidadIgual_2";
		String seq2s2Pair = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		String seq2s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq3s1 = "@ReverseMasCalidad_1";
		String seq3s2 = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1Pair = "@ReverseMasCalidad_2";
		String seq3s2Pair = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		String seq3s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		seq3.setPairSequence(seq3s1Pair, seq3s2Pair, commLine, seq3s4Pair);

		String seq1s1fa = ">Original";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq1s1faPair = ">Original";
		String seq1s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		seq1fa.setPairSequence(seq1s1faPair, seq1s2faPair);

		String seq2s1fa = ">Reverse";
		String seq2s2fa = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq2s1faPair = ">Reverse";
		String seq2s2faPair = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		seq2fa.setPairSequence(seq2s1faPair, seq2s2faPair);

		String seq3s1fa = ">ReverseCopia";
		String seq3s2fa = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		String seq3s1faPair = ">ReverseCopia";
		String seq3s2faPair = "CCCCCACACAAAAAGGCTAAACCCCCCCCCT";
		seq3fa.setPairSequence(seq3s1faPair, seq3s2faPair);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new ReverseDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);

		list = new ArrayList<>(filtered.collect());

		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
	}

	@Test
	public void filterComplementDistinct() {
		String seq1s1 = "@Original";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@Complement";
		String seq2s2 = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s1 = "@ComplementCopiaMasCalidad";
		String seq3s2 = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq1s1fa = ">Original";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq2s1fa = ">Complement";
		String seq2s2fa = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq3s1fa = ">ComplementCopia";
		String seq3s2fa = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new ComplementDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
	}

	@Test
	public void filterComplementDistinctPair() {
		String seq1s1 = "@Original_1";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@Original_2";
		String seq1s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@Complement_1";
		String seq2s2 = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@Complement_2";
		String seq2s2Pair = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		String seq2s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq3s1 = "@ComplementCopiaMasCalidad_1";
		String seq3s2 = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1Pair = "@ComplementCopiaMasCalidad_2";
		String seq3s2Pair = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		String seq3s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		seq3.setPairSequence(seq3s1Pair, seq3s2Pair, commLine, seq3s4Pair);

		String seq1s1fa = ">Original_1";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq1s1faPair = ">Original_2";
		String seq1s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		seq1fa.setPairSequence(seq1s1faPair, seq1s2faPair);

		String seq2s1fa = ">Complement_1";
		String seq2s2fa = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq2s1faPair = ">Complement_2";
		String seq2s2faPair = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		seq2fa.setPairSequence(seq2s1faPair, seq2s2faPair);

		String seq3s1fa = ">ComplementCopia_1";
		String seq3s2fa = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		String seq3s1faPair = ">ComplementCopia_2";
		String seq3s2faPair = "AGGGGGGGGGTTTAGCCTTTTTGTGTGGGGG";
		seq3fa.setPairSequence(seq3s1faPair, seq3s2faPair);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new ComplementDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
	}

	@Test
	public void filterReverseComplementDistinct() {
		String seq1s1 = "@Original";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq2s1 = "@ComplementReverseCalidadIgual";
		String seq2s2 = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq3s1 = "@ComplementReverseMasCalidad";
		String seq3s2 = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq1s1fa = ">Original";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq2s1fa = ">ComplementReverse";
		String seq2s2fa = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq3s1fa = ">ComplementReverseCopia";
		String seq3s2fa = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new ReverseComplementDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
	}

	@Test
	public void filterReverseComplementDistinctPair() {
		String seq1s1 = "@Original_1";
		String seq1s2 = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		String seq1s1Pair = "@Original_2";
		String seq1s2Pair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		String seq1s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq1.setPairSequence(seq1s1Pair, seq1s2Pair, commLine, seq1s4Pair);

		String seq2s1 = "@ComplementReverseCalidadIgual_1";
		String seq2s2 = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq2s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		String seq2s1Pair = "@ComplementReverseCalidadIgual_2";
		String seq2s2Pair = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq2s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,-";
		seq2.setPairSequence(seq2s1Pair, seq2s2Pair, commLine, seq2s4Pair);

		String seq3s1 = "@ComplementReverseMasCalidad_1";
		String seq3s2 = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq3s4 = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		String seq3s1Pair = "@ComplementReverseMasCalidad_2";
		String seq3s2Pair = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		String seq3s4Pair = "5?:5;<02:@977=:<0=9>@5>7>;>*3,1";
		seq3.setPairSequence(seq3s1Pair, seq3s2Pair, commLine, seq3s4Pair);

		String seq1s1fa = ">Original_1";
		String seq1s2fa = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		Sequence seq1fa = new Sequence(seq1s1fa, seq1s2fa);

		String seq1s1faPair = ">Original_2";
		String seq1s2faPair = "TCCCCCCCCCAAATCGGAAAAACACACCCCC";
		seq1fa.setPairSequence(seq1s1faPair, seq1s2faPair);

		String seq2s1fa = ">ComplementReverse_1";
		String seq2s2fa = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		Sequence seq2fa = new Sequence(seq2s1fa, seq2s2fa);

		String seq2s1faPair = ">ComplementReverse_2";
		String seq2s2faPair = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		seq2fa.setPairSequence(seq2s1faPair, seq2s2faPair);

		String seq3s1fa = ">ComplementReverseCopia_1";
		String seq3s2fa = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		Sequence seq3fa = new Sequence(seq3s1fa, seq3s2fa);

		String seq3s1faPair = ">ComplementReverseCopia_2";
		String seq3s2faPair = "GGGGGTGTGTTTTTCCGATTTGGGGGGGGGA";
		seq3fa.setPairSequence(seq3s1faPair, seq3s2faPair);

		JavaRDD<Sequence> original = jsc.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> originalFA = jsc.parallelize(Arrays.asList(seq1fa, seq2fa, seq3fa));
		JavaRDD<Sequence> emptyRdd = jsc.parallelize(new ArrayList<Sequence>());
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;
		GroupFilter filter = new ReverseComplementDistinct();

		// Test for empty RDD
		filtered = filter.validate(emptyRdd);
		assertEquals(filtered.count(), 0);

		filtered = filter.validate(original);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
		assertTrue(list.contains(seq3));

		filtered = filter.validate(originalFA);
		assertEquals(filtered.count(), 1);
		list = new ArrayList<>(filtered.collect());
		assertEquals(list.size(), 1);
	}

}