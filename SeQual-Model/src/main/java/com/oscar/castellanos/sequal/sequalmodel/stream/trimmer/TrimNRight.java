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

public class TrimNRight implements Trimmer {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1373653504736559043L;

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @param isPaired  is the sequences are paired or not
	 * @return the Dataset
	 */
	@Override
	public Dataset<SequenceWithTimestamp> trim(Dataset<SequenceWithTimestamp> sequences, boolean isPaired) {
		String limitStr;
		Integer limit;

		limitStr = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIM_N_RIGHT);

		if (StringUtils.isBlank(limitStr)) {
			return sequences;
		}

		limit = new Integer(limitStr);

		if (limit <= 0) {
			return sequences;
		}

		if (isPaired) {
			return sequences.map((org.apache.spark.api.java.function.MapFunction<SequenceWithTimestamp,SequenceWithTimestamp>)sequence -> this.doTrimPair(sequence, limit),Encoders.bean(SequenceWithTimestamp.class));
		}

		return sequences.map((org.apache.spark.api.java.function.MapFunction<SequenceWithTimestamp,SequenceWithTimestamp>)sequence -> this.doTrim(sequence, limit),Encoders.bean(SequenceWithTimestamp.class));
	}

	/**
	 * Do trim.
	 *
	 * @param sequence the sequence
	 * @param limit    the limit
	 * @return the sequence
	 */
	private SequenceWithTimestamp doTrim(SequenceWithTimestamp sequenceWithTimestamp, Integer limit) {
		Sequence sequence = sequenceWithTimestamp.getSequence();
		int length = sequence.getLength();
		if (length > limit) {
			String strToCheck = sequence.getSequenceString().substring(length - limit);
			if (strToCheck.matches("N{" + limit + "}")) {
				sequence.setSequenceString(sequence.getSequenceString().substring(0, length - limit));
				if (sequence.getHasQuality()) {
					sequence.setQualityString(sequence.getQualityString().substring(0, length - limit));
				}
			}
		}

		return sequenceWithTimestamp;
	}

	/**
	 * Do trim pair.
	 *
	 * @param sequence the sequence
	 * @param limit    the limit
	 * @return the sequence
	 */
	private SequenceWithTimestamp doTrimPair(SequenceWithTimestamp sequenceWithTimestamp, Integer limit) {

		this.doTrim(sequenceWithTimestamp, limit);

		Sequence sequence = sequenceWithTimestamp.getSequence();
		int length = sequence.getLengthPair();
		if (length > limit) {
			String strToCheck = sequence.getSequenceStringPair().substring(length - limit);
			if (strToCheck.matches("N{" + limit + "}")) {
				sequence.setSequenceStringPair(sequence.getSequenceStringPair().substring(0, length - limit));
				if (sequence.getHasQuality()) {
					sequence.setQualityStringPair(sequence.getQualityStringPair().substring(0, length - limit));
				}
			}
		}

		return sequenceWithTimestamp;
	}

}
