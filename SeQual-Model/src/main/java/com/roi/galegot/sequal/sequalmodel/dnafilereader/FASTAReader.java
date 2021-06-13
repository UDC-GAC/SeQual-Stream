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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

import es.udc.gac.hadoop.sequence.parser.mapreduce.FastAInputFormat;

/**
 * The Class FASTAReader.
 */
@SuppressWarnings("serial")
public class FASTAReader implements DNAFileReader {

	/**
	 * Read file to RDD.
	 *
	 * @param inFile the in file
	 * @param sc     the sc
	 * @return the java RDD
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public JavaRDD<Sequence> readFileToRDD(String inFile, JavaSparkContext sc) throws IOException {

		JavaPairRDD<LongWritable, Text> rdd = sc.newAPIHadoopFile(inFile, FastAInputFormat.class, LongWritable.class,
				Text.class, new Configuration());

		return rdd.map(tuple -> {
			String[] seq = tuple._2.toString().split("\\n");
			return new Sequence(seq[0], seq[1]);
		});
	}

}