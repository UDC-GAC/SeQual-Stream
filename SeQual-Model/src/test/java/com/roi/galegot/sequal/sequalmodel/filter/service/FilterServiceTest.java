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
package com.roi.galegot.sequal.sequalmodel.filter.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.service.FilterService;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class FilterServiceTest {

	private FilterService filterService;

	private static SparkConf spc;
	private static JavaSparkContext jsc;

	private static String commLine = "+";

	@BeforeClass
	public static void setupSpark() throws IOException {
		spc = new SparkConf().setAppName("SeQual").setMaster("local[*]");
		jsc = new JavaSparkContext(spc);
		jsc.setLogLevel("ERROR");
	}

	@Before
	public void setupService() {
		this.filterService = new FilterService();
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void filterService() {

		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCANGCAATANTCTCCGAACNGGAGAG";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		/*
		 * Duplicate from 1 with more quality Length = 30
		 */
		String seq2s1 = "@cluster_8:UMI_CTTTGA";
		String seq2s2 = "TATCCANGCAATANTCTCCGAACNGGAGAG";
		String seq2s4 = "1/04.72,(003,-2-22+00-12./.-.5";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		/*
		 * Length = 29
		 */
		String seq3s1 = "@cluster_12:UMI_GGTCAA";
		String seq3s2 = "GCAGTTNNAGATCAATTATNNNAGAGCAA";
		String seq3s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		/*
		 * Length = 28
		 */
		String seq4s1 = "@cluster_21:UMI_AGAACA";
		String seq4s2 = "GGCATTGCAAAATATNTTSCACCCCCAA";
		String seq4s4 = ">=2.660/?:36AD;0<14703640334";
		Sequence seq4 = new Sequence(seq4s1, seq4s2, commLine, seq4s4);

		/*
		 * Length = 29 Non-IUPAC character: Z
		 */
		String seq5s1 = "@cluster_21:UMI_AGAACA";
		String seq5s2 = "GGCATTGCAAAATTTNTTSCACCCCCAAZ";
		String seq5s4 = ">=2.660/?:36AD;0<147036403343";
		Sequence seq5 = new Sequence(seq5s1, seq5s2, commLine, seq5s4);

		JavaRDD<Sequence> original = jsc
				.parallelize(Arrays.asList(seq1, seq2, seq3, seq4, seq5));
		JavaRDD<Sequence> filtered;
		ArrayList<Sequence> list;

		ExecutionParametersManager.setParameter("SingleFilters", "");
		filtered = this.filterService.filter(original);

		assertEquals(5, filtered.count());
		list = new ArrayList<>(filtered.collect());
		assertEquals(5, list.size());
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq4));
		assertTrue(list.contains(seq5));

		ExecutionParametersManager.setParameter("SingleFilters", "LENGTH");
		ExecutionParametersManager.setParameter("LengthMinVal", "29");
		ExecutionParametersManager.setParameter("LengthMaxVal", "30");

		filtered = this.filterService.filter(original);

		assertEquals(4, filtered.count());
		list = new ArrayList<>(filtered.collect());
		assertEquals(4, list.size());
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq5));

		ExecutionParametersManager.setParameter("SingleFilters",
				"LENGTH | QUALITY");
		ExecutionParametersManager.setParameter("QualityMinVal", "10");
		ExecutionParametersManager.setParameter("QualityMaxVal", "29");

		filtered = this.filterService.filter(original);

		assertEquals(3, filtered.count());
		list = new ArrayList<>(filtered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq5));

		ExecutionParametersManager.setParameter("SingleFilters",
				"LENGTH | QUALITY | NONIUPAC");

		filtered = this.filterService.filter(original);

		assertEquals(2, filtered.count());
		list = new ArrayList<>(filtered.collect());
		assertEquals(2, list.size());
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));

		ExecutionParametersManager.setParameter("GroupFilters", "DISTINCT");

		filtered = this.filterService.filter(original);

		assertEquals(1, filtered.count());
		list = new ArrayList<>(filtered.collect());
		assertEquals(1, list.size());
		assertTrue(list.contains(seq2));
	}
}
