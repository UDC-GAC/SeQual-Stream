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
package com.roi.galegot.sequal.sequalmodel.formatter;

/**
 * The Enum Formatters.
 */
public enum Formatters {

	/** The dnatorna. */
	DNATORNA("com.roi.galegot.sequal.sequalmodel.formatter.DNAToRNA"),

	/** The rnatodna. */
	RNATODNA("com.roi.galegot.sequal.sequalmodel.formatter.RNAToDNA"),

	/** The fastqtofasta. */
	FASTQTOFASTA("com.roi.galegot.sequal.sequalmodel.formatter.FASTQToFASTA");

	/** The formatter class name. */
	private String formatterClassName;

	/**
	 * Instantiates a new formatters.
	 *
	 * @param formatterClassName the formatter class name
	 */
	private Formatters(String formatterClassName) {
		this.formatterClassName = formatterClassName;
	}

	/**
	 * Gets the formatter class.
	 *
	 * @return the formatter class
	 */
	public String getFormatterClass() {
		return this.formatterClassName;
	}
}