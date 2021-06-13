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
package com.roi.galegot.sequal.sequalmodel.stat;

import java.util.Arrays;
import java.util.List;

/**
 * The Class StatsNaming.
 */
public class StatsPhrasing {

	/** The Constant COUNT_BEFORE. */
	public static final String COUNT_BEFORE = "Count before transformations:";

	/** The Constant COUNT_AFTER. */
	public static final String COUNT_AFTER = "Count after transformations:";

	/** The Constant MEAN_QUALITY_BEFORE. */
	public static final String MEAN_QUALITY_BEFORE = "Mean quality before transformations:";

	/** The Constant MEAN_QUALITY_AFTER. */
	public static final String MEAN_QUALITY_AFTER = "Mean quality after transformations:";

	/** The Constant MEAN_LENGTH_BEFORE. */
	public static final String MEAN_LENGTH_BEFORE = "Mean length before transformations:";

	/** The Constant MEAN_LENGTH_AFTER. */
	public static final String MEAN_LENGTH_AFTER = "Mean length after transformations:";

	/** The Constant ORDERED_STATS. */
	public static final List<String> ORDERED_STATS = Arrays.asList(COUNT_BEFORE, MEAN_QUALITY_BEFORE,
			MEAN_LENGTH_BEFORE, COUNT_AFTER, MEAN_QUALITY_AFTER, MEAN_LENGTH_AFTER);

}
