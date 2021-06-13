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

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class TrimLeftP.
 */
public class TrimLeftP implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 3195898863006276645L;

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		String percentageStr;
		Double percentage;

		if (sequences.isEmpty()) {
			return sequences;
		}

		percentageStr = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIM_LEFTP);

		if (StringUtils.isBlank(percentageStr)) {
			return sequences;
		}

		percentage = new Double(percentageStr);

		if ((percentage <= 0) || (percentage >= 1)) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.map(sequence -> this.doTrimPair(sequence, percentage));
		}

		return sequences.map(sequence -> this.doTrim(sequence, percentage));
	}

	/**
	 * Do trim.
	 *
	 * @param sequence   the sequence
	 * @param percentage the percentage
	 * @return the sequence
	 */
	private Sequence doTrim(Sequence sequence, Double percentage) {
		Integer valueToTrim = (int) (percentage * sequence.getLength());
		sequence.setSequenceString(sequence.getSequenceString().substring(valueToTrim));
		if (sequence.getHasQuality()) {
			sequence.setQualityString(sequence.getQualityString().substring(valueToTrim));
		}

		return sequence;
	}

	/**
	 * Do trim pair.
	 *
	 * @param sequence   the sequence
	 * @param percentage the percentage
	 * @return the sequence
	 */
	private Sequence doTrimPair(Sequence sequence, Double percentage) {

		this.doTrim(sequence, percentage);

		Integer valueToTrim = (int) (percentage * sequence.getLengthPair());
		sequence.setSequenceStringPair(sequence.getSequenceStringPair().substring(valueToTrim));
		if (sequence.getHasQuality()) {
			sequence.setQualityStringPair(sequence.getQualityStringPair().substring(valueToTrim));
		}

		return sequence;
	}

}