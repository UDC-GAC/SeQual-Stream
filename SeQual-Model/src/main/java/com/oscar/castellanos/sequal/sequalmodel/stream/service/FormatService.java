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

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.oscar.castellanos.sequal.sequalmodel.stream.formatter.FASTQToFASTA;
import com.oscar.castellanos.sequal.sequalmodel.stream.formatter.Formatter;
import com.oscar.castellanos.sequal.sequalmodel.stream.formatter.FormatterFactory;
import com.oscar.castellanos.sequal.sequalmodel.stream.formatter.FormattersStream;
import com.roi.galegot.sequal.sequalmodel.formatter.FormatterParametersNaming;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

/**
 * The Class FormatService.
 */
public class FormatService {

	public boolean formattingFastqToFasta = false;
	
	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(FormatService.class.getName());

	/**
	 * Format.
	 *
	 * @param sequences  the sequences
	 * @param isPaired   if the sequences are paired or not
	 * @return the Dataset
	 */
	public Dataset<SequenceWithTimestamp> format(Dataset<SequenceWithTimestamp> sequences, boolean isPaired) {
		List<FormattersStream> formatters = this.getFormatters();
		if (!formatters.isEmpty()) {
			return this.applyFormatters(sequences, formatters, isPaired);
		} else {
			LOGGER.warn("No formatters specified. No operations will be performed.\n");
		}
		return sequences;
	}

	/**
	 * Format loop.
	 *
	 * @param sequences  the sequences
	 * @param formatters the formatters
	 * @param isPaired   if the sequences are paired or not
	 * @return the Dataset
	 */
	private Dataset<SequenceWithTimestamp> applyFormatters(Dataset<SequenceWithTimestamp> sequences, List<FormattersStream> formatters, boolean isPaired) {
		for (int i = 0; i < formatters.size(); i++) {
			Formatter formatter = FormatterFactory.getFormatter(formatters.get(i));
			
			if (formatter.getClass() == FASTQToFASTA.class) this.formattingFastqToFasta = true;
			
			LOGGER.info("Applying formatter " + formatters.get(i) + "\n");

			sequences = formatter.format(sequences, isPaired);
		}
		return sequences;
	}

	/**
	 * Gets the formatters.
	 *
	 * @return the formatters
	 */
	private List<FormattersStream> getFormatters() {
		String formatters = ExecutionParametersManager.getParameter(FormatterParametersNaming.FORMATTERS_LIST);
		ArrayList<FormattersStream> enumFormatters = new ArrayList<>();

		if (StringUtils.isNotBlank(formatters)) {
			String[] splitFormatters = formatters.split("\\|");
			for (String formatter : splitFormatters) {
				if (StringUtils.isNotBlank(formatter)) {
					enumFormatters.add(FormattersStream.valueOf(formatter.trim()));
				}
			}
		}

		return enumFormatters;
	}
	
}
