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
package com.roi.galegot.sequal.sequalmodel.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.filter.Filter;
import com.roi.galegot.sequal.sequalmodel.filter.FilterFactory;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.filter.Filters;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class FilterService.
 */
public class FilterService {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(FilterService.class.getName());

	/**
	 * Filter.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public JavaRDD<Sequence> filter(JavaRDD<Sequence> sequences) {
		List<Filters> filters = this.getFilters();
		if (filters.isEmpty()) {
			LOGGER.warn("No filters specified. No operations will be performed.\n");
		} else {
			sequences = this.applyFilters(sequences, filters);
		}
		return sequences;
	}

	/**
	 * Retrieves the specified filters into ExecutionParameter.properties file and
	 * converts them into Filters
	 *
	 * @return List<Filters> containing all the specified filters
	 * @see filters.Filters
	 */
	private List<Filters> getFilters() {
		String filters;
		String[] splitFilters;
		Map<Integer, Filters> filtersMap;

		filtersMap = new TreeMap<Integer, Filters>();

		filters = ExecutionParametersManager.getParameter(FilterParametersNaming.SINGLE_FILTERS_LIST);
		if (StringUtils.isNotBlank(filters)) {
			splitFilters = filters.split("\\|");
			for (String filter : splitFilters) {
				if (StringUtils.isNotBlank(filter)) {
					Filters filterEntity = Filters.valueOf(filter.trim());
					filtersMap.put(filterEntity.getPriority(), filterEntity);
				}
			}
		}

		filters = ExecutionParametersManager.getParameter(FilterParametersNaming.GROUP_FILTERS_LIST);
		if (StringUtils.isNotBlank(filters)) {
			splitFilters = filters.split("\\|");
			for (String filter : splitFilters) {
				if (StringUtils.isNotBlank(filter)) {
					Filters filterEntity = Filters.valueOf(filter.trim());
					filtersMap.put(filterEntity.getPriority(), filterEntity);
				}
			}
		}

		return new ArrayList<Filters>(filtersMap.values());
	}

	/**
	 * Apply filters.
	 *
	 * @param sequences the sequences
	 * @param filters   the filters
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> applyFilters(JavaRDD<Sequence> sequences, List<Filters> filters) {
		for (int i = 0; i < filters.size(); i++) {
			if (sequences.isEmpty()) {
				return sequences;
			}
			Filter filter = FilterFactory.getFilter(filters.get(i));

			LOGGER.info("Applying filter " + filters.get(i) + "\n");

			sequences = filter.validate(sequences);
		}

		return sequences;
	}

}