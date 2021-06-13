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
package com.roi.galegot.sequal.sequalmodel.filter.single;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class GCContent.
 */
public class GCContent implements SingleFilter {

	private static final long serialVersionUID = 4373618581457549681L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		Double limMin;
		Double limMax;

		String limMinStr;
		String limMaxStr;

		Boolean limMinUse;
		Boolean limMaxUse;

		if (sequences.isEmpty()) {
			return sequences;
		}

		limMinStr = ExecutionParametersManager.getParameter(FilterParametersNaming.GCCONTENT_MIN_VAL);
		limMaxStr = ExecutionParametersManager.getParameter(FilterParametersNaming.GCCONTENT_MAX_VAL);
		limMinUse = StringUtils.isNotBlank(limMinStr);
		limMaxUse = StringUtils.isNotBlank(limMaxStr);

		limMin = (limMinUse) ? new Double(limMinStr) : null;
		limMax = (limMaxUse) ? new Double(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.filter(sequence -> this.filter(sequence, limMin, limMinUse, limMax, limMaxUse)
					&& this.filterPair(sequence, limMin, limMinUse, limMax, limMaxUse));
		}

		return sequences.filter(sequence -> this.filter(sequence, limMin, limMinUse, limMax, limMaxUse));
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
		return this.compare(sequence.getGuaCytP(), limMin, limMinUse, limMax, limMaxUse);
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
		return this.compare(sequence.getGuaCytPPair(), limMin, limMinUse, limMax, limMaxUse);
	}

	/**
	 * Compare.
	 *
	 * @param guaCytP   the gua cyt P
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean compare(double guaCytP, Double limMin, Boolean limMinUse, Double limMax, Boolean limMaxUse) {

		if (limMinUse && limMaxUse) {
			return ((guaCytP >= limMin) && (guaCytP <= limMax));
		}
		if (limMinUse) {
			return (guaCytP >= limMin);
		}
		if (limMaxUse) {
			return (guaCytP <= limMax);
		}

		return true;
	}
}