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
 * The Class ComplementDistinct.
 */
public class ComplementDistinct implements GroupFilter {

	private static final long serialVersionUID = -5947189310641527595L;

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
		JavaPairRDD<ComplementString, Sequence> group;

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
	private JavaPairRDD<ComplementString, Sequence> filterSingleEnd(JavaRDD<Sequence> sequences) {
		return sequences.mapToPair(sequence -> new Tuple2<ComplementString, Sequence>(
				new ComplementString(sequence.getSequenceString()), sequence));
	}

	/**
	 * Filter pair.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaPairRDD<ComplementString, Sequence> filterPairedEnd(JavaRDD<Sequence> sequences) {
		return sequences.mapToPair(sequence -> new Tuple2<ComplementString, Sequence>(
				new ComplementStringPair(sequence.getSequenceString(), sequence.getSequenceStringPair()), sequence));
	}

	/**
	 * Filter quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterQuality(JavaPairRDD<ComplementString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> SequenceUtils.selectSequenceWithMaxQuality(seq1, seq2)).values();
	}

	/**
	 * Filter no quality.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> filterNoQuality(JavaPairRDD<ComplementString, Sequence> sequences) {
		return sequences.reduceByKey((seq1, seq2) -> seq1).values();
	}

	/**
	 * The Class ComplementString.
	 */
	class ComplementString implements Serializable {

		private static final long serialVersionUID = 3011027347808993546L;

		protected String sequence;
		protected String complementarySequence;

		/**
		 * Instantiates a new complement string.
		 *
		 * @param sequence the sequence
		 */
		public ComplementString(String sequence) {
			this.sequence = sequence;
			this.complementarySequence = this.getComplementary(sequence);
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
					+ ((this.complementarySequence == null) ? 0 : this.complementarySequence.hashCode()));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ComplementString other = (ComplementString) obj;

			return this.sequence.equals(other.complementarySequence);
		}
	}

	/**
	 * The Class ComplementStringPair.
	 */
	class ComplementStringPair extends ComplementString {

		private static final long serialVersionUID = -7291101312113278780L;

		private String sequencePair;
		private String complementarySequencePair;

		/**
		 * Instantiates a new complement string pair.
		 *
		 * @param sequence     the sequence
		 * @param sequencePair the sequence pair
		 */
		public ComplementStringPair(String sequence, String sequencePair) {
			super(sequence);
			this.sequencePair = sequencePair;
			this.complementarySequencePair = super.getComplementary(sequencePair);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + (((this.sequence == null) ? 0 : this.sequence.hashCode())
					+ ((this.complementarySequence == null) ? 0 : this.complementarySequence.hashCode()));
			result = (prime * result) + (((this.sequencePair == null) ? 0 : this.sequencePair.hashCode())
					+ ((this.complementarySequencePair == null) ? 0 : this.complementarySequencePair.hashCode()));

			return result;
		}

		@Override
		public boolean equals(Object obj) {
			ComplementStringPair other = (ComplementStringPair) obj;

			return (this.sequence.equals(other.complementarySequence)
					&& this.sequencePair.equals(other.complementarySequencePair));
		}

	}

}