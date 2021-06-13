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
package com.oscar.castellanos.sequal.sequalmodel.stream.dnafilereader;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;

public interface DNAFileReader extends Serializable {

	/**
	 * Reads a file (or files) and returns a Dataset of Sequences with timestamp based on its content
	 *
	 * @param args Parameters received at main call, important not to modify them
	 * @param session  Session for the Spark App
	 * @return Dataset<SequenceWithTimestamp> created from file (or files) content
	 * @throws IOException
	 */
	public Dataset<SequenceWithTimestamp> readFileToDataset(String inFile, SparkSession session) throws IOException;

}