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

import com.roi.galegot.sequal.sequalmodel.exceptions.NonExistentStatException;

/**
 * A factory for creating Stat objects.
 */
public class StatFactory {

	/**
	 * Instantiates a new stat factory.
	 */
	private StatFactory() {
	}

	/**
	 * Returns a Stat based on the enum Stats.
	 *
	 * @param stat Stat selected to be returned
	 * @return Stat specified
	 */
	public static synchronized Stat getStat(Stats stat) {
		try {
			return (Stat) Class.forName(stat.getStatClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new NonExistentStatException(stat.getStatClass());
		}
	}
}