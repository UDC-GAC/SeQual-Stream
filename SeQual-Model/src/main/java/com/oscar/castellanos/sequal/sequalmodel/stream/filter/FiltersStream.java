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
package com.oscar.castellanos.sequal.sequalmodel.stream.filter;

public enum FiltersStream {

	/** The length. */
	LENGTH(0, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.Length"),

	/** The qualityscore. */
	QUALITYSCORE(1, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.QualityScore"),

	/** The quality. */
	QUALITY(2, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.Quality"),

	/** The gc. */
	GCBASES(3, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.GCBases"),

	/** The gcp. */
	GCCONTENT(4, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.GCContent"),

	/** The namb. */
	NAMB(5, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.NAmb"),

	/** The nambp. */
	NAMBP(6, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.NAmbP"),

	/** The noniupac. */
	NONIUPAC(7, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.NonIupac"),

	/** The pattern. */
	PATTERN(8, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.Pattern"),

	/** The nopattern. */
	NOPATTERN(9, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.NoPattern"),

	/** The basen. */
	BASEN(10, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.BaseN"),

	/** The basep. */
	BASEP(11, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.single.BaseP"),

	/** The distinct. */
	DISTINCT(12, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.group.Distinct"),

	/** The almostdistinct. */
	ALMOSTDISTINCT(13, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.group.AlmostDistinct"),

	/** The reversedistinct. */
	REVERSEDISTINCT(14, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.group.ReverseDistinct"),

	/** The complementdistinct. */
	COMPLEMENTDISTINCT(15, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.group.ComplementDistinct"),

	/** The reversecomplementdistinct. */
	REVERSECOMPLEMENTDISTINCT(16, "com.oscar.castellanos.sequal.sequalmodel.stream.filter.group.ReverseComplementDistinct");

	/** The Constant MAX_FILTER_PRIORITY. */
	public static final int MAX_FILTER_PRIORITY = 16;

	/** The filter class name. */
	private String filterClassName;

	/** The priority. */
	private int priority;

	/**
	 * Instantiates a new filters.
	 *
	 * @param priority        the priority
	 * @param filterClassName the filter class name
	 */
	private FiltersStream(int priority, String filterClassName) {
		this.priority = priority;
		this.filterClassName = filterClassName;
	}

	/**
	 * Gets the filter class.
	 *
	 * @return the filter class
	 */
	public String getFilterClass() {
		return this.filterClassName;
	}

	/**
	 * Gets the priority.
	 *
	 * @return the priority
	 */
	public int getPriority() {
		return this.priority;
	}
	
}
