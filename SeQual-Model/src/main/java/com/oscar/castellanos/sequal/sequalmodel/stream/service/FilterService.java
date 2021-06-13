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
package com.oscar.castellanos.sequal.sequalmodel.stream.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.oscar.castellanos.sequal.sequalmodel.stream.filter.FQFilter;
import com.oscar.castellanos.sequal.sequalmodel.stream.filter.Filter;
import com.oscar.castellanos.sequal.sequalmodel.stream.filter.FilterFactory;
import com.oscar.castellanos.sequal.sequalmodel.stream.filter.FiltersStream;
import com.roi.galegot.sequal.sequalmodel.filter.FilterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class FilterService {
	
	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(FilterService.class.getName());

	/**
	 * Filter.
	 *
	 * @param sequences the sequences
	 * @param isPaired   if the sequences are paired or not
	 * @param hasQuality if the sequences are in fastq format or not
	 * @return the Dataset
	 */
	public Dataset<SequenceWithTimestamp> filter(Dataset<SequenceWithTimestamp> sequences, 
			boolean isPaired, boolean hasQuality) {

		List<FiltersStream> filters = this.getFilters();
		if (filters.isEmpty()) {
			LOGGER.warn("No filters specified. No operations will be performed.\n");
		} else {
			sequences = this.applyFilters(sequences, filters, isPaired, hasQuality);
		}
		return sequences;
	}

	/**
	 * Retrieves the specified filters into ExecutionParameter.properties file and
	 * converts them into Filters
	 *
	 * @return List<Filters> containing all the specified filters
	 * @see FiltersStream.Filters
	 */
	private List<FiltersStream> getFilters() {
		String filters;
		String[] splitFilters;
		Map<Integer, FiltersStream> filtersMap;

		filtersMap = new TreeMap<Integer, FiltersStream>();

		filters = ExecutionParametersManager.getParameter(FilterParametersNaming.SINGLE_FILTERS_LIST);
		if (StringUtils.isNotBlank(filters)) {
			splitFilters = filters.split("\\|");
			for (String filter : splitFilters) {
				if (StringUtils.isNotBlank(filter)) {
					FiltersStream filterEntity = FiltersStream.valueOf(filter.trim());
					filtersMap.put(filterEntity.getPriority(), filterEntity);
				}
			}
		}

		filters = ExecutionParametersManager.getParameter(FilterParametersNaming.GROUP_FILTERS_LIST);
		if (StringUtils.isNotBlank(filters)) {
			splitFilters = filters.split("\\|");
			for (String filter : splitFilters) {
				if (StringUtils.isNotBlank(filter)) {
					FiltersStream filterEntity = FiltersStream.valueOf(filter.trim());
					filtersMap.put(filterEntity.getPriority(), filterEntity);
				}
			}
		}

		return new ArrayList<FiltersStream>(filtersMap.values());
	}

	/**
	 * Apply filters.
	 *
	 * @param sequences the sequences
	 * @param filters   the filters
	 * @param isPaired   if the sequences are paired or not
	 * @param hasQuality if the sequences are in fastq format or not
	 * @return the Dataset
	 */
	private Dataset<SequenceWithTimestamp> applyFilters(Dataset<SequenceWithTimestamp> sequences, 
			List<FiltersStream> filters, boolean isPaired, boolean hasQuality) {
		for (int i = 0; i < filters.size(); i++) {

			Filter filter = FilterFactory.getFilter(filters.get(i));

			LOGGER.info("Applying filter " + filters.get(i) + "\n");
			
			if (filter instanceof FQFilter) {
				((FQFilter) filter).setHasQuality(hasQuality);
			}
			
			sequences = filter.validate(sequences,isPaired);
		}

		return sequences;
	}
	
}
