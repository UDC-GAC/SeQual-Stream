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
 * The Class ReverseComplementDistinct.
 */
public class ReverseComplementDistinct implements GroupFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -3416801873476204891L;

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
		JavaPairRDD<ReverseComplementString, Sequence> group;

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
	private JavaPairRDD<ReverseComplementString, Sequence> filterSingleEnd(JavaRDD<Sequence> sequences) {
		return sequences.mapToPair(sequence -> new Tuple2<ReverseComplementString, Sequence>(
				new ReverseComplementString(sequence.getSequenceString()), sequence));
	}

	/**
	 * Filter pair.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaPairRDD<ReverseComplementString, Sequence> filterPairedEnd(JavaRDD<Sequence> sequences) {
		return sequences.mapToPair(sequence -> new Tuple2<ReverseComplementString, Sequence>(
				new ReverseComplementStringPair(sequence.getSequenceString(), sequence.getSequenceStringPair()),
				sequence));
	}

	/**
	 * Filter quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterQuality(JavaPairRDD<ReverseComplementString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> SequenceUtils.selectSequenceWithMaxQuality(seq1, seq2)).values();
	}

	/**
	 * Filter no quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterNoQuality(JavaPairRDD<ReverseComplementString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> seq1).values();
	}

	/**
	 * The Class ComplementString.
	 */
	class ReverseComplementString implements Serializable {

		private static final long serialVersionUID = 3011027347808993546L;

		protected String sequence;
		protected String reverseComplementarySequence;

		/**
		 * Instantiates a new complement string.
		 *
		 * @param sequence the sequence
		 */
		public ReverseComplementString(String sequence) {
			this.sequence = sequence;
			this.reverseComplementarySequence = new StringBuilder(this.getComplementary(sequence)).reverse().toString();
		}

		/**
		 * Gets the complementary.
		 *
		 * @param sequence the sequence
		 * @return the complementary
		 */
		private String getComplementary(String sequence) {
			char[] result = new char[sequence.length()];
			int counter = 0;
			for (char c : this.sequence.toCharArray()) {
				switch (c) {
				case 'A':
					result[counter] = 'T';
					break;
				case 'G':
					result[counter] = 'C';
					break;
				case 'T':
					result[counter] = 'A';
					break;
				case 'C':
					result[counter] = 'G';
					break;
				default:
					result[counter] = c;
				}
				counter++;
			}
			return String.valueOf(result);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (((this.sequence == null) ? 0 : this.sequence.hashCode())
					+ ((this.reverseComplementarySequence == null) ? 0 : this.reverseComplementarySequence.hashCode()));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ReverseComplementString other = (ReverseComplementString) obj;

			return this.sequence.equals(other.reverseComplementarySequence);
		}
	}

	/**
	 * The Class ComplementStringPair.
	 */
	class ReverseComplementStringPair extends ReverseComplementString {

		private static final long serialVersionUID = -7291101312113278780L;

		private String sequencePair;
		private String reverseComplementarySequencePair;

		/**
		 * Instantiates a new complement string pair.
		 *
		 * @param sequence     the sequence
		 * @param sequencePair the sequence pair
		 */
		public ReverseComplementStringPair(String sequence, String sequencePair) {
			super(sequence);
			this.sequencePair = sequencePair;
			this.reverseComplementarySequencePair = new StringBuilder(super.getComplementary(sequencePair)).reverse()
					.toString();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (((this.sequence == null) ? 0 : this.sequence.hashCode())
					+ ((this.reverseComplementarySequence == null) ? 0 : this.reverseComplementarySequence.hashCode()));
			result = (prime * result) + (((this.sequencePair == null) ? 0 : this.sequencePair.hashCode())
					+ ((this.reverseComplementarySequencePair == null) ? 0
							: this.reverseComplementarySequencePair.hashCode()));

			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ReverseComplementStringPair other = (ReverseComplementStringPair) obj;

			return (this.sequence.equals(other.reverseComplementarySequence)
					&& this.sequencePair.equals(other.reverseComplementarySequencePair));
		}

	}

}