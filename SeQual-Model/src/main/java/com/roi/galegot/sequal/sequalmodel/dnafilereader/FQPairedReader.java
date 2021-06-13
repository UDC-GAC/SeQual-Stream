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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;

import es.udc.gac.hadoop.sequence.parser.mapreduce.FastQInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceInputFormat;
import es.udc.gac.hadoop.sequence.parser.mapreduce.PairedEndSequenceRecordReader;

/**
 * The Class FQReader.
 */
@SuppressWarnings("serial")
public class FQPairedReader implements DNAPairedFileReader {

	/**
	 * Read file to RDD.
	 *
	 * @param inFile1      the in file 1
	 * @param inFile2      the in file 2
	 * @param sparkContext the spark context
	 * @return the java RDD
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public JavaRDD<Sequence> readFileToRDD(String inFile1, String inFile2, JavaSparkContext sparkContext)
			throws IOException {

		Configuration hadoopConf = sparkContext.hadoopConfiguration();
		Path inputPath1 = new Path(inFile1);
		Path inputPath2 = new Path(inFile2);

//		Set left and right input paths for HSP
		PairedEndSequenceInputFormat.setLeftInputPath(hadoopConf, inputPath1, FastQInputFormat.class);
		PairedEndSequenceInputFormat.setRightInputPath(hadoopConf, inputPath2, FastQInputFormat.class);

		JavaPairRDD<LongWritable, Text> rdd = sparkContext.newAPIHadoopFile(inputPath1.getName(),
				PairedEndSequenceInputFormat.class, LongWritable.class, Text.class, hadoopConf);

		return rdd.map(tuple -> {
			String[] seq1 = PairedEndSequenceRecordReader.getLeftRead(tuple._1, tuple._2).split("\\n");
			String[] seq2 = PairedEndSequenceRecordReader.getRightRead(tuple._1, tuple._2).split("\\n");
			Sequence seq = new Sequence(seq1[0], seq1[1], seq1[2], seq1[3]);
			seq.setPairSequence(seq2[0], seq2[1], seq2[2], seq2[3]);

			return seq;
		});
	}
}