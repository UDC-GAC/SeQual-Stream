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
package com.roi.galegot.sequal.sequalmodel.trim.service;

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
import com.roi.galegot.sequal.sequalmodel.service.TrimService;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class TrimServiceTest {

	private TrimService trimService;

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
		this.trimService = new TrimService();
	}

	@AfterClass
	public static void stopSpark() {
		jsc.close();
	}

	@Test
	public void trimService() {
		/*
		 * Length = 30
		 */
		String seq1s1 = "@cluster_8:UMI_CTTTGA";
		String seq1s2 = "TATCCUNGCAATANTCTCCGAACNGNNNNN";
		String seq1s4 = "1/04.72,(003,-2-22+00-12./.-.4";
		Sequence seq1 = new Sequence(seq1s1, seq1s2, commLine, seq1s4);

		// Copy of above sequence with 5 characters
		String seq4s2 = "CTCCGAACNGNNNNN";
		String seq4s4 = "-22+00-12./.-.4";
		Sequence seq4 = new Sequence(seq1s1, seq4s2, commLine, seq4s4);

		// Copy of above sequence with 5 characters
		String seq7s2 = "CTCCGAACNGNN";
		String seq7s4 = "-22+00-12./.";
		Sequence seq7 = new Sequence(seq1s1, seq7s2, commLine, seq7s4);

		/*
		 * Length = 29
		 */
		String seq2s1 = "@cluster_12:UMI_GGTCAA";
		String seq2s2 = "GCAGTTNNAGATCAATATATNNNAGAGCA";
		String seq2s4 = "?7?AEEC@>=1?A?EEEB9ECB?==:B.A";
		Sequence seq2 = new Sequence(seq2s1, seq2s2, commLine, seq2s4);

		// Copy of above sequence with 4 characters
		String seq5s2 = "TATATNNNAGAGCA";
		String seq5s4 = "EEB9ECB?==:B.A";
		Sequence seq5 = new Sequence(seq2s1, seq5s2, commLine, seq5s4);

		/*
		 * Length = 24
		 */
		String seq3s1 = "@cluster_21:UMI_AGAACA";
		String seq3s2 = "GGCATTGCAANNNN";
		String seq3s4 = ">=2.660/?:36AD";
		Sequence seq3 = new Sequence(seq3s1, seq3s2, commLine, seq3s4);

		// Copy of above sequence with 4 characters
		String seq6s2 = "GGCATTGCAAN";
		String seq6s4 = ">=2.660/?:3";
		Sequence seq6 = new Sequence(seq3s1, seq6s2, commLine, seq6s4);

		JavaRDD<Sequence> original = jsc
				.parallelize(Arrays.asList(seq1, seq2, seq3));
		JavaRDD<Sequence> trimmered;
		ArrayList<Sequence> list;

		ExecutionParametersManager.setParameter("Trimmers", "");
		trimmered = this.trimService.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq1));
		assertTrue(list.contains(seq2));
		assertTrue(list.contains(seq3));

		ExecutionParametersManager.setParameter("Trimmers", "TRIMLEFT");
		ExecutionParametersManager.setParameter("TrimLeft", "15");
		trimmered = this.trimService.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq3));
		assertTrue(list.contains(seq4));
		assertTrue(list.contains(seq5));

		ExecutionParametersManager.setParameter("Trimmers",
				"TRIMLEFT|TRIMNRIGHT");
		ExecutionParametersManager.setParameter("TrimNRight", "3");
		trimmered = this.trimService.trim(original);
		assertEquals(3, trimmered.count());
		list = new ArrayList<>(trimmered.collect());
		assertEquals(3, list.size());
		assertTrue(list.contains(seq5));
		assertTrue(list.contains(seq6));
		assertTrue(list.contains(seq7));
	}
}
