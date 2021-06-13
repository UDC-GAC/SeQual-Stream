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
package com.roi.galegot.sequal.sequalmodel.trimmer;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class TrimRightToLength.
 */
public class TrimRightToLength implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1953745377281159367L;

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		String limitStr;
		Integer limit;

		if (sequences.isEmpty()) {
			return sequences;
		}

		limitStr = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIM_RIGHT_TO_LENGTH);

		if (StringUtils.isBlank(limitStr)) {
			return sequences;
		}

		limit = new Integer(limitStr);

		if (limit <= 0) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.map(sequence -> this.doTrimPair(sequence, limit));
		}

		return sequences.map(sequence -> this.doTrim(sequence, limit));
	}

	/**
	 * Do trim.
	 *
	 * @param sequence the sequence
	 * @param limit    the limit
	 * @return the sequence
	 */
	private Sequence doTrim(Sequence sequence, Integer limit) {
		if (sequence.getLength() > limit) {
			sequence.setSequenceString(sequence.getSequenceString().substring(0, limit));
			if (sequence.getHasQuality()) {
				sequence.setQualityString(sequence.getQualityString().substring(0, limit));
			}
		}

		return sequence;
	}

	/**
	 * Do trim pair.
	 *
	 * @param sequence the sequence
	 * @param limit    the limit
	 * @return the sequence
	 */
	private Sequence doTrimPair(Sequence sequence, Integer limit) {

		this.doTrim(sequence, limit);

		if (sequence.getLengthPair() > limit) {
			sequence.setSequenceStringPair(sequence.getSequenceStringPair().substring(0, limit));
			if (sequence.getHasQuality()) {
				sequence.setQualityStringPair(sequence.getQualityStringPair().substring(0, limit));
			}
		}

		return sequence;
	}
}
