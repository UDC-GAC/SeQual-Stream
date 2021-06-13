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
package com.roi.galegot.sequal.sequalmodel.exceptions;

/**
 * The Class NonExistentFilterException.
 */
@SuppressWarnings("serial")
public class NonExistentFilterException extends RuntimeException {

	/**
	 * Instantiates a new non existent filter exception.
	 *
	 * @param filter the filter
	 */
	public NonExistentFilterException(String filter) {
		super("Specified filter " + filter + " does not exist.");
	}
}