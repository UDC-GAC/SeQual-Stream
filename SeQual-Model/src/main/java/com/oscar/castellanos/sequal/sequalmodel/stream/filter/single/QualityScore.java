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
package com.oscar.castellanos.sequal.sequalmodel.stream.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.oscar.castellanos.sequal.sequalmodel.stream.filter.FQFilter;
import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class QualityScore extends FQFilter implements SingleFilter {

	private static final long serialVersionUID = 1986175935593684878L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @param isPaired  if the sequences are paired or not
	 * @return the Dataset
	 */
	@Override
	public Dataset<SequenceWithTimestamp> validate(Dataset<SequenceWithTimestamp> sequences, boolean isPaired) {
		Double limMin;
		Double limMax;

		String limMinStr;
		String limMaxStr;

		Boolean limMinUse;
		Boolean limMaxUse;

		limMinStr = ExecutionParametersManager.getParameter(FilterParametersNaming.QUALITY_SCORE_MIN_VAL);
		limMaxStr = ExecutionParametersManager.getParameter(FilterParametersNaming.QUALITY_SCORE_MAX_VAL);
		limMinUse = StringUtils.isNotBlank(limMinStr);
		limMaxUse = StringUtils.isNotBlank(limMaxStr);

		limMin = (limMinUse) ? new Double(limMinStr) : null;
		limMax = (limMaxUse) ? new Double(limMaxStr) : null;

		if ((!limMinUse && !limMaxUse) || !this.hasQuality) {
			return sequences;
		}

		if (isPaired) {
			return sequences.filter((org.apache.spark.api.java.function.FilterFunction<SequenceWithTimestamp>)s -> this.filter(s.getSequence(), limMin, limMinUse, limMax, limMaxUse)
					&& this.filterPair(s.getSequence(), limMin, limMinUse, limMax, limMaxUse));
		}

		return sequences.filter((org.apache.spark.api.java.function.FilterFunction<SequenceWithTimestamp>)s -> this.filter(s.getSequence(), limMin, limMinUse, limMax, limMaxUse));
	}

	/**
	 * Filter.
	 *
	 * @param sequence  the sequence
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filter(Sequence sequence, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (sequence.getHasQuality()) {
			return this.compare(sequence.getQualityString(), limMin, limMinUse, limMax, limMaxUse);
		}

		return false;
	}

	/**
	 * Filter pair.
	 *
	 * @param sequence  the sequence
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filterPair(Sequence sequence, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (sequence.getHasQuality()) {
			return this.compare(sequence.getQualityStringPair(), limMin, limMinUse, limMax, limMaxUse);
		}

		return false;
	}

	/**
	 * Compare.
	 *
	 * @param qualityString the quality string
	 * @param limMin        the lim min
	 * @param limMinUse     the lim min use
	 * @param limMax        the lim max
	 * @param limMaxUse     the lim max use
	 * @return the boolean
	 */
	private Boolean compare(String qualityString, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {
		if (limMinUse && limMaxUse) {
			for (char c : qualityString.toCharArray()) {
				int qual = c - 33;
				if ((qual > limMax) || (qual < limMin)) {
					return false;
				}
			}
		}
		if (limMinUse) {
			for (char c : qualityString.toCharArray()) {
				int qual = c - 33;
				if (qual < limMin) {
					return false;
				}
			}
		}
		if (limMaxUse) {
			for (char c : qualityString.toCharArray()) {
				int qual = c - 33;
				if (qual > limMax) {
					return false;
				}
			}
		}

		return true;
	}

}
