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

import com.roi.galegot.sequal.sequalmodel.exceptions.NonExistentFormatterException;

/**
 * A factory for creating Formatter objects.
 */
public class FormatterFactory {

	/**
	 * Instantiates a new formatter factory.
	 */
	private FormatterFactory() {
	}

	/**
	 * Returns a Formatter based on the enum Formatters.
	 *
	 * @param formatter Formatter selected to be returned
	 * @return Formatter specified
	 */
	public static synchronized Formatter getFormatter(Formatters formatter) {
		try {
			return (Formatter) Class.forName(formatter.getFormatterClass())
					.newInstance();
		} catch (ClassNotFoundException | InstantiationException
				| IllegalAccessException e) {
			throw new NonExistentFormatterException(
					formatter.getFormatterClass());
		}
	}
}