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
import com.oscar.castellanos.sequal.sequalmodel.stream.reader.FASTAPairedReaderUtils;
import com.oscar.castellanos.sequal.sequalmodel.stream.utils.StreamConstants;
import com.roi.galegot.sequal.sequalmodel.common.Sequence;

@SuppressWarnings("serial")
public class FASTAPairedReader implements DNAPairedFileReader{

	@Override
	public Dataset<SequenceWithTimestamp> readFileToDataset(String inFile1, String inFile2, SparkSession sparkSession)
			throws IOException {
		
		String inputDir = StreamConstants.INPUT_DIR;
		
		FASTAPairedReaderUtils reader = new FASTAPairedReaderUtils();
		reader.tryOpenFilePaired(inFile1, inFile2);
		reader.readAndWriteDirectory(inFile1, inFile2, inputDir, sparkSession);
				
	    Dataset<Row> lines = sparkSession
	    	      .readStream()
	    	      .option("lineSep", reader.SEQUENCE_PAIR_NAME_PREFIX)
	    	      .text(inputDir);
		
	    System.out.println("---- Reading stream ----\n");
	    
	    Dataset<SequenceWithTimestamp> seqs = lines
		    	.filter("value != ''")
		    	.map( (org.apache.spark.api.java.function.MapFunction<Row,SequenceWithTimestamp>) tuple -> {		    		
					String[] sequence = tuple.toString().split("\\n");
					sequence = (String[]) ArrayUtils.remove(sequence, sequence.length-1);
					
					String basesLines = "";
					int i = 1;
					while (i < sequence.length - 1 && !sequence[i].startsWith(">")) {
						basesLines += sequence[i];
						if (!sequence[i+1].startsWith(">")) basesLines += "\n";  
						i = i + 1;
					}

					// now i is in position of sequence pair name
					String pairBasesLines = "";
					int j = i+1;	

					while (j < sequence.length - 1) {
						pairBasesLines += sequence[j];
						if (j < sequence.length - 2) pairBasesLines += "\n"; 
						j = j + 1;
					}				
						
					Sequence s = new Sequence(sequence[0].substring(1), basesLines);
					s.setPairSequence(sequence[i], pairBasesLines);
					return new SequenceWithTimestamp(s,sequence[sequence.length-1]);
			}, Encoders.bean(SequenceWithTimestamp.class));
	    	    
	    return seqs;
	}	
	
}
