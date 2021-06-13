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
package com.oscar.castellanos.sequal.sequalmodel.stream.trimmer;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.trimmer.TrimmerParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class TrimRightP implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -4460342240618286511L;

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @param isPaired  is the sequences are paired or not
	 * @return the Dataset
	 */
	@Override
	public Dataset<SequenceWithTimestamp> trim(Dataset<SequenceWithTimestamp> sequences, boolean isPaired) {
		String percentageStr;
		Double percentage;

		percentageStr = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIM_RIGHTP);

		if (StringUtils.isBlank(percentageStr)) {
			return sequences;
		}

		percentage = new Double(percentageStr);

		if ((percentage <= 0) || (percentage >= 1)) {
			return sequences;
		}

		if (isPaired) {
			return sequences.map((org.apache.spark.api.java.function.MapFunction<SequenceWithTimestamp,SequenceWithTimestamp>)sequence -> this.doTrimPair(sequence, percentage),Encoders.bean(SequenceWithTimestamp.class));
		}

		return sequences.map((org.apache.spark.api.java.function.MapFunction<SequenceWithTimestamp,SequenceWithTimestamp>)sequence -> this.doTrim(sequence, percentage),Encoders.bean(SequenceWithTimestamp.class));
	}

	/**
	 * Do trim.
	 *
	 * @param sequence   the sequence
	 * @param percentage the percentage
	 * @return the sequence
	 */
	private SequenceWithTimestamp doTrim(SequenceWithTimestamp sequenceWithTimestamp, Double percentage) {
		Sequence sequence = sequenceWithTimestamp.getSequence();
		Integer oldLength = sequence.getLength();
		Integer valueToTrim = (int) (percentage * sequence.getLength());
		sequence.setSequenceString(sequence.getSequenceString().substring(0, oldLength - valueToTrim));
		if (sequence.getHasQuality()) {
			sequence.setQualityString(sequence.getQualityString().substring(0, oldLength - valueToTrim));
		}

		return sequenceWithTimestamp;
	}

	/**
	 * Do trim pair.
	 *
	 * @param sequence   the sequence
	 * @param percentage the percentage
	 * @return the sequence
	 */
	private SequenceWithTimestamp doTrimPair(SequenceWithTimestamp sequenceWithTimestamp, Double percentage) {

		this.doTrim(sequenceWithTimestamp, percentage);

		Sequence sequence = sequenceWithTimestamp.getSequence();
		Integer oldLength = sequence.getLengthPair();
		Integer valueToTrim = (int) (percentage * sequence.getLengthPair());
		sequence.setSequenceStringPair(sequence.getSequenceStringPair().substring(0, oldLength - valueToTrim));
		if (sequence.getHasQuality()) {
			sequence.setQualityStringPair(sequence.getQualityStringPair().substring(0, oldLength - valueToTrim));
		}

		return sequenceWithTimestamp;
	}

}
