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
import com.roi.galegot.sequal.sequalmodel.trimmer.Trimmer;
import com.roi.galegot.sequal.sequalmodel.trimmer.TrimmerFactory;
import com.roi.galegot.sequal.sequalmodel.trimmer.TrimmerParametersNaming;
import com.roi.galegot.sequal.sequalmodel.trimmer.Trimmers;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class TrimService.
 */
public class TrimService {

	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(TrimService.class.getName());

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @return the java RDD
	 */
	public JavaRDD<Sequence> trim(JavaRDD<Sequence> sequences) {
		List<Trimmers> trimmers = this.getTrimmers();
		if (!trimmers.isEmpty()) {
			return this.applyTrimmers(sequences, trimmers);
		} else {
			LOGGER.warn("No trimmers specified. No operations will be performed.\n");
		}
		return sequences;
	}

	/**
	 * Apply trimmers.
	 *
	 * @param sequences the sequences
	 * @param trimmers  the trimmers
	 * @return the java RDD
	 */
	private JavaRDD<Sequence> applyTrimmers(JavaRDD<Sequence> sequences, List<Trimmers> trimmers) {
		for (int i = 0; i < trimmers.size(); i++) {
			if (sequences.isEmpty()) {
				return sequences;
			}

			LOGGER.info("Applying trimmer " + trimmers.get(i) + "\n");

			Trimmer trimmer = TrimmerFactory.getTrimmer(trimmers.get(i));
			sequences = trimmer.trim(sequences);
		}
		return sequences;
	}

	/**
	 * Gets the trimmers.
	 *
	 * @return the trimmers
	 */
	private List<Trimmers> getTrimmers() {
		String trimmers;
		String[] splitTrimmers;
		Map<Integer, Trimmers> trimmersMap;

		trimmersMap = new TreeMap<>();

		trimmers = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIMMERS_LIST);
		if (StringUtils.isNotBlank(trimmers)) {
			splitTrimmers = trimmers.split("\\|");
			for (String trimmer : splitTrimmers) {
				if (StringUtils.isNotBlank(trimmer)) {
					Trimmers trimmerEntity = Trimmers.valueOf(trimmer.trim());
					trimmersMap.put(trimmerEntity.getPriority(), trimmerEntity);
				}
			}
		}

		return new ArrayList<>(trimmersMap.values());
	}

}