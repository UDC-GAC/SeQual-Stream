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
package com.roi.galegot.sequal.sequalmodel.dnafilereader;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

public interface DNAFileReader extends Serializable {

	/**
	 * Reads a file (or files) and returns a RDD of Sequences based on its content
	 *
	 * @param args Parameters received at main call, important not to modify them
	 * @param jsc  Context for the Spark App
	 * @return JavaRDD<Sequence> created from file (or files) content
	 * @throws IOException
	 */
	public JavaRDD<Sequence> readFileToRDD(String inFile, JavaSparkContext jsc) throws IOException;

}