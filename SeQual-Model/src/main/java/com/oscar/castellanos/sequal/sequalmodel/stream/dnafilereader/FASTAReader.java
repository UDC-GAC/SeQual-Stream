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
import com.oscar.castellanos.sequal.sequalmodel.stream.reader.FASTAReaderUtils;
import com.oscar.castellanos.sequal.sequalmodel.stream.utils.StreamConstants;
import com.roi.galegot.sequal.sequalmodel.common.Sequence;

@SuppressWarnings("serial")
public class FASTAReader implements DNAFileReader {

	@Override
	public Dataset<SequenceWithTimestamp> readFileToDataset(String inFile, SparkSession sparkSession) throws IOException {
		
		String inputDir = StreamConstants.INPUT_DIR;
		
		FASTAReaderUtils reader = new FASTAReaderUtils();
		reader.tryOpenFile(inFile);
		reader.readAndWriteDirectory(inFile, inputDir, sparkSession);

	    Dataset<Row> lines = sparkSession
	    	      .readStream()
	    	      .option("lineSep", reader.SEQUENCE_NAME_PREFIX)
	    	      .text(inputDir);

	    System.out.println("---- Reading stream ----\n");
	    
	    Dataset<SequenceWithTimestamp> seqs = lines
		    	.filter("value != ''")
		    	.map( (org.apache.spark.api.java.function.MapFunction<Row,SequenceWithTimestamp>)  tuple -> {
					String[] sequence = tuple.toString().split("\\n");
					sequence = (String[]) ArrayUtils.remove(sequence, sequence.length-1);
					
					String basesLines = "";	
					for (int i = 1; i < sequence.length -1; i++) {
						basesLines += sequence[i];
						if (i < sequence.length - 2) basesLines += "\n";
					}

					Sequence s = new Sequence(sequence[0].substring(1), basesLines);
					return new SequenceWithTimestamp(s,sequence[sequence.length-1]);
					
			}, Encoders.bean(SequenceWithTimestamp.class));
	    
	    return seqs;
	}	
	
}
