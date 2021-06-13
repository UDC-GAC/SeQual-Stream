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
import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.exceptions.InvalidNumberOfParametersException;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class BaseP implements SingleFilter {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -3632770663351055062L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @param isPaired  if the sequences are paired or not
	 * @return the Dataset
	 */
	@Override
	public Dataset<SequenceWithTimestamp> validate(Dataset<SequenceWithTimestamp> sequences, boolean isPaired) {
		String[] bases;
		String[] baseMin;
		String[] baseMax;

		String basesMinStr;
		String basesMaxStr;

		Boolean limMaxUse;
		Boolean limMinUse;

		String basesStr = ExecutionParametersManager.getParameter(FilterParametersNaming.BASEP);
		if (StringUtils.isNotBlank(basesStr)) {
			bases = basesStr.split("\\|");
		} else {
			return sequences;
		}

		basesMinStr = ExecutionParametersManager.getParameter(FilterParametersNaming.BASEP_MIN_VAL);
		if (StringUtils.isNotBlank(basesMinStr)) {
			baseMin = basesMinStr.split("\\|");
			limMinUse = true;
		} else {
			baseMin = null;
			limMinUse = false;
		}

		basesMaxStr = ExecutionParametersManager.getParameter(FilterParametersNaming.BASEP_MAX_VAL);
		if (StringUtils.isNotBlank(basesMaxStr)) {
			baseMax = basesMaxStr.split("\\|");
			limMaxUse = true;
		} else {
			baseMax = null;
			limMaxUse = false;
		}

		if (!limMinUse && !limMaxUse) {
			return sequences;
		}

		if ((limMinUse && (bases.length != baseMin.length)) || (limMaxUse && (bases.length != baseMax.length))) {
			throw new InvalidNumberOfParametersException();
		}

		if (isPaired) {
			return sequences.filter((org.apache.spark.api.java.function.FilterFunction<SequenceWithTimestamp>)s -> this.filter(s.getSequence(), bases, baseMin, limMinUse, baseMax, limMaxUse)
					&& this.filterPair(s.getSequence(), bases, baseMin, limMinUse, baseMax, limMaxUse));
		}

		return sequences.filter((org.apache.spark.api.java.function.FilterFunction<SequenceWithTimestamp>)s -> this.filter(s.getSequence(), bases, baseMin, limMinUse, baseMax, limMaxUse));
	}

	/**
	 * Filter.
	 *
	 * @param sequence  the sequence
	 * @param bases     the bases
	 * @param baseMin   the base min
	 * @param limMinUse the lim min use
	 * @param baseMax   the base max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filter(Sequence sequence, String[] bases, String[] baseMin, Boolean limMinUse, String[] baseMax,
			Boolean limMaxUse) {
		return this.compare(sequence.getSequenceString(), bases, baseMin, limMinUse, baseMax, limMaxUse);
	}

	/**
	 * Filter pair.
	 *
	 * @param sequence  the sequence
	 * @param bases     the bases
	 * @param baseMin   the base min
	 * @param limMinUse the lim min use
	 * @param baseMax   the base max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean filterPair(Sequence sequence, String[] bases, String[] baseMin, Boolean limMinUse, String[] baseMax,
			Boolean limMaxUse) {
		return this.compare(sequence.getSequenceStringPair(), bases, baseMin, limMinUse, baseMax, limMaxUse);
	}

	/**
	 * Compare.
	 *
	 * @param sequenceString the sequence string
	 * @param bases          the bases
	 * @param baseMin        the base min
	 * @param limMinUse      the lim min use
	 * @param baseMax        the base max
	 * @param limMaxUse      the lim max use
	 * @return the boolean
	 */
	private Boolean compare(String sequenceString, String[] bases, String[] baseMin, Boolean limMinUse,
			String[] baseMax, Boolean limMaxUse) {

		Double lim1;
		Double lim2;

		for (int i = 0; i < bases.length; i++) {
			Double perc = (double) StringUtils.countMatches(sequenceString, bases[i]) / sequenceString.length();
			if (limMinUse) {
				lim1 = new Double(baseMin[i]);
				if ((lim1 != -1) && (perc < lim1)) {
					return false;
				}
			}
			if (limMaxUse) {
				lim2 = new Double(baseMax[i]);
				if ((lim2 != -1) && (perc > lim2)) {
					return false;
				}
			}
		}

		return true;
	}
}
