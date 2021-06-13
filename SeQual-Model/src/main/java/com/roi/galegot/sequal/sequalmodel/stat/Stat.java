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

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

public interface Stat extends Serializable {

	/**
	 * Measure.
	 *
	 * @param seqs the seqs
	 * @return the double
	 */
	public Double measure(JavaRDD<Sequence> seqs);

	/**
	 * Measure pair.
	 *
	 * @param seqs the seqs
	 * @return the double
	 */
	public Double measurePair(JavaRDD<Sequence> seqs);

}