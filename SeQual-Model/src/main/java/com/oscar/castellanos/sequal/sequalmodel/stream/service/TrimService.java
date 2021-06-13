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
import org.apache.spark.sql.Dataset;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.oscar.castellanos.sequal.sequalmodel.stream.trimmer.FQTrimmer;
import com.oscar.castellanos.sequal.sequalmodel.stream.trimmer.Trimmer;
import com.oscar.castellanos.sequal.sequalmodel.stream.trimmer.TrimmerFactory;
import com.oscar.castellanos.sequal.sequalmodel.stream.trimmer.TrimmersStream;
import com.roi.galegot.sequal.sequalmodel.trimmer.TrimmerParametersNaming;
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
	 * @param sequences  the sequences
	 * @param isPaired   if the sequences are paired or not
	 * @param hasQuality if the sequences are in fastq format or not
	 * @return the Dataset
	 */
	public Dataset<SequenceWithTimestamp> trim(Dataset<SequenceWithTimestamp> sequences, 
			boolean isPaired, boolean hasQuality) {
		
		List<TrimmersStream> trimmers = this.getTrimmers();
		if (!trimmers.isEmpty()) {
			return this.applyTrimmers(sequences, trimmers, isPaired, hasQuality);
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
	 * @param isPaired   if the sequences are paired or not
	 * @param hasQuality if the sequences are in fastq format or not
	 * @return the Dataset
	 */
	private Dataset<SequenceWithTimestamp> applyTrimmers(Dataset<SequenceWithTimestamp> sequences,
			List<TrimmersStream> trimmers, boolean isPaired, boolean hasQuality) {
		for (int i = 0; i < trimmers.size(); i++) {

			LOGGER.info("Applying trimmer " + trimmers.get(i) + "\n");

			Trimmer trimmer = TrimmerFactory.getTrimmer(trimmers.get(i));
			
			if (trimmer instanceof FQTrimmer) {
				((FQTrimmer) trimmer).setHasQuality(hasQuality);
			}
			
			sequences = trimmer.trim(sequences,isPaired);
		}
		return sequences;
	}

	/**
	 * Gets the trimmers.
	 *
	 * @return the trimmers
	 */
	private List<TrimmersStream> getTrimmers() {
		String trimmers;
		String[] splitTrimmers;
		Map<Integer, TrimmersStream> trimmersMap;

		trimmersMap = new TreeMap<>();

		trimmers = ExecutionParametersManager.getParameter(TrimmerParametersNaming.TRIMMERS_LIST);
		if (StringUtils.isNotBlank(trimmers)) {
			splitTrimmers = trimmers.split("\\|");
			for (String trimmer : splitTrimmers) {
				if (StringUtils.isNotBlank(trimmer)) {
					TrimmersStream trimmerEntity = TrimmersStream.valueOf(trimmer.trim());
					trimmersMap.put(trimmerEntity.getPriority(), trimmerEntity);
				}
			}
		}

		return new ArrayList<>(trimmersMap.values());
	}
	
}
