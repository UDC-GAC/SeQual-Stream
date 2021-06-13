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
 * The Class GCBases.
 */
public class GCBases implements SingleFilter {

	private static final long serialVersionUID = 8628696067644155529L;

	/**
	 * Validate.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	@Override
	public JavaRDD<Sequence> validate(JavaRDD<Sequence> sequences) {
		Integer limMin;
		Integer limMax;

		String limMinStr;
		String limMaxStr;

		Boolean limMinUse;
		Boolean limMaxUse;

		if (sequences.isEmpty()) {
			return sequences;
		}

		limMinStr = ExecutionParametersManager.getParameter(FilterParametersNaming.GCBASES_MIN_VAL);
		limMaxStr = ExecutionParametersManager.getParameter(FilterParametersNaming.GCBASES_MAX_VAL);
		limMinUse = StringUtils.isNotBlank(limMinStr);
		limMaxUse = StringUtils.isNotBlank(limMaxStr);

		limMin = (limMinUse) ? new Integer(limMinStr) : null;
		limMax = (limMaxUse) ? new Integer(limMaxStr) : null;

		if (!limMinUse && !limMaxUse) {
			return sequences;
		}

		if (sequences.first().getIsPaired()) {
			return sequences.filter(s -> this.filter(s, limMin, limMinUse, limMax, limMaxUse)
					&& this.filterPair(s, limMin, limMinUse, limMax, limMaxUse));
		}

		return sequences.filter(s -> this.filter(s, limMin, limMinUse, limMax, limMaxUse));
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
	private Boolean filter(Sequence sequence, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {
		return this.compare(sequence.getGuaCyt(), limMin, limMinUse, limMax, limMaxUse);
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
	private Boolean filterPair(Sequence sequence, Integer limMin, Boolean limMinUse, Integer limMax,
			Boolean limMaxUse) {
		return this.compare(sequence.getGuaCytPair(), limMin, limMinUse, limMax, limMaxUse);
	}

	/**
	 * Compare.
	 *
	 * @param guaCyt    the gua cyt
	 * @param limMin    the lim min
	 * @param limMinUse the lim min use
	 * @param limMax    the lim max
	 * @param limMaxUse the lim max use
	 * @return the boolean
	 */
	private Boolean compare(int guaCyt, Integer limMin, Boolean limMinUse, Integer limMax, Boolean limMaxUse) {

		if (limMinUse && limMaxUse) {
			return ((guaCyt >= limMin) && (guaCyt <= limMax));
		}
		if (limMinUse) {
			return (guaCyt >= limMin);
		}
		if (limMaxUse) {
			return (guaCyt <= limMax);
		}

		return true;
	}
}