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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.common.SequenceUtils;

import scala.Tuple2;

/**
 * The Class ReverseDistinct.
 */
public class ReverseDistinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1061447508688116490L;

	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		if (sequences.isEmpty()) {
			return sequences;
		}

		return this.filter(sequences);
	}

	/**
	 * Filter.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filter(JavaRDD<Sequence> sequences) {
		JavaPairRDD<ReverseString, Sequence> group;

		if (sequences.first().getIsPaired()) {
			group = this.filterPairedEnd(sequences);
		} else {
			group = this.filterSingleEnd(sequences);
		}

		if (sequences.first().getHasQuality()) {
			return this.filterQuality(group);
		}

		return this.filterNoQuality(group);
	}

	/**
	 * Filter single end.
	 *
	 * @param sequences the sequences
	 * @return the java pair RDD
	 */
	private JavaPairRDD<ReverseString, Sequence> filterSingleEnd(JavaRDD<Sequence> sequences) {
		return sequences.mapToPair(sequence -> new Tuple2<ReverseString, Sequence>(
				new ReverseString(sequence.getSequenceString()), sequence));
	}

	/**
	 * Filter pair.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaPairRDD<ReverseString, Sequence> filterPairedEnd(JavaRDD<Sequence> sequences) {
		return sequences.mapToPair(sequence -> new Tuple2<ReverseString, Sequence>(
				new ReverseStringPair(sequence.getSequenceString(), sequence.getSequenceStringPair()), sequence));
	}

	/**
	 * Filter quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterQuality(JavaPairRDD<ReverseString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> SequenceUtils.selectSequenceWithMaxQuality(seq1, seq2)).values();
	}

	/**
	 * Filter no quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterNoQuality(JavaPairRDD<ReverseString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> seq1).values();
	}

	/**
	 * The Class ReverseString.
	 */
	class ReverseString implements Serializable {

		private static final long serialVersionUID = -3467393373228813336L;

		protected String sequence;
		protected String reverseSequence;

		/**
		 * Instantiates a new reverse string.
		 *
		 * @param sequence the sequence
		 */
		public ReverseString(String sequence) {
			this.sequence = sequence;
			this.reverseSequence = new StringBuilder(sequence).reverse().toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (((this.sequence == null) ? 0 : this.sequence.hashCode())
					+ ((this.reverseSequence == null) ? 0 : this.reverseSequence.hashCode()));

			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ReverseString other = (ReverseString) obj;

			return this.sequence.equals(other.reverseSequence);
		}

	}

	/**
	 * The Class ReverseStringPair.
	 */
	class ReverseStringPair extends ReverseString {

		private static final long serialVersionUID = -7291101312113278780L;

		private String sequencePair;
		private String reverseSequencePair;

		/**
		 * Instantiates a new reverse string pair.
		 *
		 * @param sequence     the sequence
		 * @param sequencePair the sequence pair
		 */
		public ReverseStringPair(String sequence, String sequencePair) {
			super(sequence);
			this.sequencePair = sequencePair;
			this.reverseSequencePair = new StringBuilder(sequencePair).reverse().toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (((this.sequence == null) ? 0 : this.sequence.hashCode())
					+ ((this.reverseSequence == null) ? 0 : this.reverseSequence.hashCode()));
			result = (prime * result) + (((this.sequencePair == null) ? 0 : this.sequencePair.hashCode())
					+ ((this.reverseSequencePair == null) ? 0 : this.reverseSequencePair.hashCode()));

			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ReverseStringPair other = (ReverseStringPair) obj;

			return (this.sequence.equals(other.reverseSequence) && this.sequencePair.equals(other.reverseSequencePair));
		}

	}

}