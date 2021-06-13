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
package com.oscar.castellanos.sequal.sequalmodel.stream.utils;

public final class StreamConstants {

	private StreamConstants() {
	}
	
	/** A random number string used to differentiate some temporal files between different executions *
	 * Useful when executing more than 1 program at once
	 */
	public static final String RANDOM_NUM = String.valueOf(Math.random()).substring(2);
	
	/** The folder name where spark is going to read sequence files. */
	public static final String INPUT_DIR = "input-sequences-" + RANDOM_NUM;
		
}
