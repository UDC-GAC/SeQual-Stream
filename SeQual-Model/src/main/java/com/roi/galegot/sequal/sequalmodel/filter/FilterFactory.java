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
package com.roi.galegot.sequal.sequalmodel.filter;

import com.roi.galegot.sequal.sequalmodel.exceptions.NonExistentFilterException;

/**
 * A factory for creating Filter objects.
 */
public class FilterFactory {

	/**
	 * Instantiates a new filter factory.
	 */
	private FilterFactory() {
	}

	/**
	 * Returns a filter based on the enum Filters.
	 *
	 * @param filter Filter selected to be returned
	 * @return SingleFilter specified
	 */
	public static synchronized Filter getFilter(Filters filter) {
		try {
			return (Filter) Class.forName(filter.getFilterClass()).newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			throw new NonExistentFilterException(filter.getFilterClass());
		}
	}
}
