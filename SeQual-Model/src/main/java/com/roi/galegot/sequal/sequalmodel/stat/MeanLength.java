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
package com.roi.galegot.sequal.sequalmodel.stat;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

/**
 * The Class MeanLength.
 */
public class MeanLength implements Stat {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5049048767496389502L;

	/**
	 * Measure.
	 *
	 * @param sequences the sequences
	 * @return the double
	 */
	@Override
	public Double measure(JavaRDD<Sequence> sequences) {

		BigDecimal mean;
		DummyCount initialDummy = new DummyCount();
		DummyCount resultDummy;
		Function2<DummyCount, Sequence, DummyCount> addAndCount;
		Function2<DummyCount, DummyCount, DummyCount> combine;

		if (sequences.isEmpty()) {
			return Double.valueOf(0);
		}

		addAndCount = new Function2<DummyCount, Sequence, DummyCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public DummyCount call(DummyCount v1, Sequence v2) throws Exception {
				v1.countLength += v2.getLength();
				v1.countNumber = v1.countNumber + Long.valueOf(1);

				return v1;
			}

		};

		combine = new Function2<DummyCount, DummyCount, DummyCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public DummyCount call(DummyCount v1, DummyCount v2) throws Exception {
				return MeanLength.this.combineFunction(v1, v2);
			}

		};

		resultDummy = sequences.aggregate(initialDummy, addAndCount, combine);
		mean = resultDummy.getMean();

		return mean.doubleValue();
	}

	/**
	 * Measure pair.
	 *
	 * @param sequences the sequences
	 * @return the double
	 */
	@Override
	public Double measurePair(JavaRDD<Sequence> sequences) {

		BigDecimal mean;
		DummyCount initialDummy = new DummyCount();
		DummyCount resultDummy;
		Function2<DummyCount, Sequence, DummyCount> addAndCount;
		Function2<DummyCount, DummyCount, DummyCount> combine;

		addAndCount = new Function2<DummyCount, Sequence, DummyCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public DummyCount call(DummyCount v1, Sequence v2) throws Exception {
				v1.countLength += v2.getLengthPair();
				v1.countNumber = v1.countNumber + Long.valueOf(1);

				return v1;
			}

		};

		combine = new Function2<DummyCount, DummyCount, DummyCount>() {

			private static final long serialVersionUID = 1L;

			@Override
			public DummyCount call(DummyCount v1, DummyCount v2) throws Exception {
				return MeanLength.this.combineFunction(v1, v2);
			}

		};

		resultDummy = sequences.aggregate(initialDummy, addAndCount, combine);
		mean = resultDummy.getMean();

		return mean.doubleValue();
	}

	/**
	 * Combine function.
	 *
	 * @param v1 the v 1
	 * @param v2 the v 2
	 * @return the dummy count
	 * @throws Exception the exception
	 */
	private DummyCount combineFunction(DummyCount v1, DummyCount v2) {
		v1.countLength += v2.countLength;
		v1.countNumber += v2.countNumber;

		return v1;
	}

	/**
	 * The Class DummyCount.
	 */
	private class DummyCount implements Serializable {

		private static final long serialVersionUID = -7774902974273299201L;
		private Long countLength = Long.valueOf(0);
		private Long countNumber = Long.valueOf(0);

		/**
		 * Gets the mean.
		 *
		 * @return the mean
		 */
		public BigDecimal getMean() {
			if (this.countNumber <= 0) {
				return BigDecimal.ZERO;
			}

			return BigDecimal.valueOf(this.countLength).divide(BigDecimal.valueOf(this.countNumber), 2,
					BigDecimal.ROUND_UP);
		}

	}

}