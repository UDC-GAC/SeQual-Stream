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

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.oscar.castellanos.sequal.sequalmodel.stream.reader.FQPairedReaderUtils;
import com.oscar.castellanos.sequal.sequalmodel.stream.utils.StreamConstants;
import com.roi.galegot.sequal.sequalmodel.common.Sequence;

@SuppressWarnings("serial")
public class FQPairedReader implements DNAPairedFileReader{

	@Override
	public Dataset<SequenceWithTimestamp> readFileToDataset(String inFile1, String inFile2, SparkSession sparkSession)
			throws IOException {
		
		String inputDir = StreamConstants.INPUT_DIR;
		
		FQPairedReaderUtils reader = new FQPairedReaderUtils();
		reader.tryOpenFilePaired(inFile1, inFile2);
		reader.readAndWriteDirectory(inFile1, inFile2, inputDir, sparkSession);
				
	    Dataset<Row> lines = sparkSession
	    	      .readStream()
	    	      .option("lineSep", reader.SEQUENCE_PAIR_NAME_PREFIX)
	    	      .text(inputDir);
		
	    System.out.println("---- Reading stream ----\n");
	    
	    Dataset<SequenceWithTimestamp> seqs = lines
		    	.filter("value != ''")
		    	.map((org.apache.spark.api.java.function.MapFunction<Row,SequenceWithTimestamp>) tuple -> {
					String[] sequence = tuple.toString().split("\\n");
					sequence = (String[]) ArrayUtils.remove(sequence, sequence.length-1);
					
					Sequence s = new Sequence(sequence[0].substring(1), sequence[2], sequence[4], sequence[6]);
					s.setPairSequence(sequence[1], sequence[3], sequence[5], sequence[7]);
					return new SequenceWithTimestamp(s,sequence[8]);
					
			}, Encoders.bean(SequenceWithTimestamp.class));
    
	    return seqs;
	}

}
