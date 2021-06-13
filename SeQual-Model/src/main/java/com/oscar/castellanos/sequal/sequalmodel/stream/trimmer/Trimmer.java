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
package com.oscar.castellanos.sequal.sequalmodel.stream.trimmer;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.roi.galegot.sequal.sequalmodel.common.Sequence;

/**
 * The Interface Trimmer.
 */
public interface Trimmer extends Serializable {

	/**
	 * Trim.
	 *
	 * @param sequences the sequences
	 * @param isPaired  is the sequences are paired or not
	 * @return the Dataset
	 */
	public Dataset<SequenceWithTimestamp> trim(Dataset<SequenceWithTimestamp> sequences, boolean isPaired);

}