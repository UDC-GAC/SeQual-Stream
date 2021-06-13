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

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.common.SequenceUtils;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

import scala.Tuple2;

/**
 * The Class AlmostDistinct.
 */
public class AlmostDistinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 3561681376850129189L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		String sDiff = ExecutionParametersManager.getParameter(FilterParametersNaming.MAX_DIFFERENCE);

		Integer maxDiff = (StringUtils.isNotBlank(sDiff)) ? new Integer(sDiff) : 0;

		return this.filter(sequences, maxDiff);
	}

	/**
	 * Filter.
	 *
	 * @param sequences     the sequences
	 * @param maxDifference the max difference
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filter(JavaRDD<Sequence> sequences, Integer maxDifference) {
		JavaPairRDD<AlmostString, Sequence> group;

		if (sequences.first().getIsPaired()) {
			group = this.filterPairedEnd(sequences, maxDifference);
		} else {
			group = this.filterSingleEnd(sequences, maxDifference);
		}

		if (sequences.first().getHasQuality()) {
			return this.filterQuality(group);
		}

		return this.filterNoQuality(group);
	}

	/**
	 * Filter single.
	 *
	 * @param sequences     the sequences
	 * @param maxDifference the max difference
	 * @return the java RDD
	 */
	private JavaPairRDD<AlmostString, Sequence> filterSingleEnd(JavaRDD<Sequence> sequences, Integer maxDifference) {
		return sequences.mapToPair(sequence -> new Tuple2<AlmostString, Sequence>(
				new AlmostString(sequence.getSequenceString(), maxDifference), sequence));
	}

	/**
	 * Filter pair.
	 *
	 * @return the java RDD
	 */
	private JavaPairRDD<AlmostString, Sequence> filterPairedEnd(JavaRDD<Sequence> sequences, Integer maxDifference) {
		return sequences.mapToPair(sequence -> new Tuple2<AlmostString, Sequence>(
				new AlmostStringPair(sequence.getSequenceString(), sequence.getSequenceStringPair(), maxDifference),
				sequence));
	}

	/**
	 * Filter quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterQuality(JavaPairRDD<AlmostString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> SequenceUtils.selectSequenceWithMaxQuality(seq1, seq2)).values();
	}

	/**
	 * Filter no quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterNoQuality(JavaPairRDD<AlmostString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> seq1).values();
	}

	/**
	 * The Class AlmostString.
	 */
	class AlmostString implements Serializable {

		private static final long serialVersionUID = 3595254670141051507L;

		protected String sequence;
		private int maxDiff;

		/**
		 * Instantiates a new almost string.
		 *
		 * @param sequence the sequence
		 * @param maxDiff  the max diff
		 */
		public AlmostString(String sequence, int maxDiff) {
			this.sequence = sequence;
			this.maxDiff = maxDiff;
		}

		@Override
		public int hashCode() {
			return 1;
		}

		@Override
		public boolean equals(Object obj) {
			AlmostString other = (AlmostString) obj;
			if (!this.checkSeq(other.sequence)) {
				return false;
			}
			return true;
		}

		/**
		 * Check seq.
		 *
		 * @param s2 the s 2
		 * @return the boolean
		 */
		private Boolean checkSeq(String s2) {
			if (this.sequence.length() != s2.length()) {
				return false;
			}

			if ((this.maxDiff <= 0) || (this.maxDiff >= this.sequence.length())) {
				return this.sequence.equals(s2);
			}

			int counter = 0;
			int diffs = 0;

			for (char c : this.sequence.toCharArray()) {
				if (c != s2.charAt(counter)) {
					diffs++;
				}
				if (diffs > this.maxDiff) {
					return false;
				}
				counter++;
			}

			return true;

		}

	}

	/**
	 * The Class AlmostString.
	 */
	class AlmostStringPair extends AlmostString {

		private static final long serialVersionUID = -7291101312113278780L;

		private String sequencePair;

		/**
		 * Instantiates a new almost string pair.
		 *
		 * @param sequence     the sequence
		 * @param sequencePair the sequence pair
		 * @param maxDiff      the max diff
		 */
		public AlmostStringPair(String sequence, String sequencePair, int maxDiff) {
			super(sequence, maxDiff);
			this.sequencePair = sequencePair;
		}

		@Override
		public boolean equals(Object obj) {
			AlmostStringPair other = (AlmostStringPair) obj;

			if (!super.checkSeq(other.sequence) || !this.checkSeqPair(other.sequencePair)) {
				return false;
			}

			return true;
		}

		/**
		 * Check seq pair.
		 *
		 * @param s2 the s 2
		 * @return the boolean
		 */
		private Boolean checkSeqPair(String s2) {
			if (this.sequencePair.length() != s2.length()) {
				return false;
			}

			if ((super.maxDiff <= 0) || (super.maxDiff >= this.sequencePair.length())) {
				return this.sequencePair.equals(s2);
			}

			int counter = 0;
			int diffs = 0;
			for (char c : this.sequencePair.toCharArray()) {
				if (c != s2.charAt(counter)) {
					diffs++;
				}

				if (diffs > super.maxDiff) {
					return false;
				}

				counter++;
			}

			return true;
		}

	}
}