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
package com.roi.galegot.sequal.sequalmodel.trimmer;

import com.roi.galegot.sequal.sequalmodel.exceptions.NonExistentTrimmerException;

/**
 * A factory for creating Trimmer objects.
 */
public class TrimmerFactory {

	/**
	 * Instantiates a new trimmer factory.
	 */
	private TrimmerFactory() {
	}

	/**
	 * Returns a Trimmer based on the enum Trimmers.
	 *
	 * @param trimmer Trimmer selected to be returned
	 * @return Trimmer specified
	 */
	public static synchronized Trimmer getTrimmer(Trimmers trimmer) {
		try {
			return (Trimmer) Class.forName(trimmer.getTrimmerClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new NonExistentTrimmerException(trimmer.getTrimmerClass());
		}
	}
}