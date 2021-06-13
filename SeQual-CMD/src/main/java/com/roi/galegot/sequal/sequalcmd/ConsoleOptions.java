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
package com.roi.galegot.sequal.sequalcmd;

/**
 * The Enum ConsoleOptions.
 */
public enum ConsoleOptions {

	/** The generateconfigfile. */
	GENERATECONFIGFILE("-g"),

	/** The filter. */
	FILTER("-f"),

	/** The input. */
	INPUT("-i"),

	/** The doubleinput. */
	DOUBLEINPUT("-di"),

	/** The output. */
	OUTPUT("-o"),

	/** The singlefileoutput. */
	SINGLEFILEOUTPUT("-sfo"),

	/** The outputstats. */
	OUTPUTSTATS("-os"),

	/** The configfile. */
	CONFIGFILE("-c"),

	/** The trim. */
	TRIM("-t"),

	/** The measure. */
	MEASURE("-s"),

	/** The format. */
	FORMAT("-fo"),

	/** The sparkmasterconf. */
	SPARKMASTERCONF("-smc"),

	/** The logconf. */
	LOGCONF("-lc");

	/** The opt. */
	private String opt;

	/**
	 * Instantiates a new console options.
	 *
	 * @param opt the opt
	 */
	ConsoleOptions(String opt) {
		this.opt = opt;
	}

	/**
	 * Gets the opt.
	 *
	 * @return the opt
	 */
	public String getOpt() {
		return this.opt;
	}
}
