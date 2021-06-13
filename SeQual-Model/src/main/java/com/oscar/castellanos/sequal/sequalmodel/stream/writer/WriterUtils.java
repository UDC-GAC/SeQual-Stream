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
package com.oscar.castellanos.sequal.sequalmodel.stream.writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.oscar.castellanos.sequal.sequalmodel.stream.utils.ControlFinishReader;
import com.oscar.castellanos.sequal.sequalmodel.stream.utils.StreamConstants;
import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.common.SequenceUtils;

public class WriterUtils {

	/** The output folder name first pair. */
	private static final String OUTPUT_FOLDER_NAME_FIRST_PAIR = "/FirstPairs";

	/** The output folder name second pair. */
	private static final String OUTPUT_FOLDER_NAME_SECOND_PAIR = "/SecondPairs";

	/**
	 * Instantiates a new writer utils.
	 */
	public WriterUtils() {
	}

	/**
	 * Write to file.
	 *
	 * @param folderPath     the folder path
	 * @param outputFileName the output file name
	 * @param partsFolder    the parts folder
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void mergeHDFSToFile(String folderPath, String outputFileName, String partsFolder)
			throws IOException {

		File merged = new File(folderPath + "/" + outputFileName);
		merged.getParentFile().mkdirs();
		if (!merged.exists()) {
			merged.createNewFile();
		}

		PrintWriter printWriter = new PrintWriter(merged);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf) ;	
	
		// Reading from folders like 'timestamp=x-y'
		PathFilter filter = new GlobFilter("*timestamp=*"); 
		FileStatus[] folderList = fs.listStatus(new Path(partsFolder),filter);
		
		Arrays.sort(folderList, getFolderComparator());
		
		for (FileStatus directory: folderList) {
			
			if (directory.isDirectory()) {
				
				FileStatus[] partList = fs.listStatus(directory.getPath());
				Arrays.sort(partList, getFileComparator());
				
				for (FileStatus file: partList) {
					
					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
					
					String line = "";
					while ((line = bufferedReader.readLine()) != null) {
						printWriter.println(line);
					}

					bufferedReader.close();
				}
				
			}
		}
		printWriter.flush();
		printWriter.close();
	}

	/**
	 * Write HDFS.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 * @param isPaired  if the sequences are paired or not
	 * @throws IOException 
	 */
	public static void writeHDFS(Dataset<SequenceWithTimestamp> sequences, String output, boolean isPaired) throws IOException {
		if (isPaired) {
			writeHDFSPaired(sequences, output);
		} else {
			
			StructType structType = getOutputStructType();
			Dataset<Row> seqs = sequences.flatMap(getOutputFlatMapFunction(),RowEncoder.apply(structType));
		    
			String checkpointDir = "file_sink_checkpoint_" + StreamConstants.RANDOM_NUM;
		
			try {
				StreamingQuery query = seqs.writeStream()
						.outputMode("append")
						.format("text")
						.option("path", output)
						.option("checkpointLocation", checkpointDir)
						.partitionBy("timestamp")
						.start();
				
				ControlFinishReader control = ControlFinishReader.getControl();	
				
				while (query.isActive()) {
					if (!query.status().isDataAvailable() 
							&& !query.status().isTriggerActive()
							&& !query.status().message().equals("Initializing sources")
							&& control.isFlag()) {
						
						query.stop();
						
		    			FileSystem fs = FileSystem.get(new Configuration());
		    			writeSuccessFile(fs,output);
		    			deleteSparkMetadata(fs,output);
						
						deleteExtraFiles(StreamConstants.INPUT_DIR,checkpointDir);
					}		
					query.awaitTermination(5000);
				}	
			
			} catch (StreamingQueryException | TimeoutException e) {
				System.out.print(" --- Streaming failure \n --- ");
				throw new IOException(e);
			}	
		}
	}

	/**
	 * Write HDFS and merge to file.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 * @param filename  the filename
	 * @param isPaired  if the sequences are paired or not
	 * @param format    the format
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void writeHDFSAndMergeToFile(Dataset<SequenceWithTimestamp> sequences, String output, 
			String filename, boolean isPaired, String format) throws IOException {

		  if (isPaired) {
			String partsFolder1 = output + OUTPUT_FOLDER_NAME_FIRST_PAIR;
			String partsFolder2 = output + OUTPUT_FOLDER_NAME_SECOND_PAIR;

			writeHDFSPaired(sequences, output);
			mergeHDFSToFile(output, filename + "_1-results." + format, partsFolder1);
			mergeHDFSToFile(output, filename + "_2-results." + format, partsFolder2);
			
		} else {
			String partsFolder = output + "/Parts";

			writeHDFS(sequences, partsFolder, isPaired);
			
			mergeHDFSToFile(output, filename + "-results." + format, partsFolder);
		}

	}

	/**
	 * Write HDFS paired.
	 *
	 * @param sequences the sequences
	 * @param output    the output
	 * @throws IOException 
	 */
	private static void writeHDFSPaired(Dataset<SequenceWithTimestamp> sequences, String output) throws IOException {
		Dataset<SequenceWithTimestamp> firstSequences;
		Dataset<SequenceWithTimestamp> secondSequences;
		
		firstSequences = sequences.map( (org.apache.spark.api.java.function.MapFunction<SequenceWithTimestamp,SequenceWithTimestamp>) sequence -> {
			Sequence s = SequenceUtils.getFirstSequenceFromPair(sequence.getSequence());
			sequence.setSequence(s);
			return sequence;
		}, Encoders.bean(SequenceWithTimestamp.class));
		
		secondSequences = sequences.map( (org.apache.spark.api.java.function.MapFunction<SequenceWithTimestamp,SequenceWithTimestamp>)sequence -> {
			Sequence s = SequenceUtils.getSecondSequenceFromPair(sequence.getSequence());
			sequence.setSequence(s);
			return sequence;
		}, Encoders.bean(SequenceWithTimestamp.class));	
		
		
		StructType structType = getOutputStructType();

		Dataset<Row> seqs1 = firstSequences.flatMap(getOutputFlatMapFunction(),RowEncoder.apply(structType));
		Dataset<Row> seqs2 = secondSequences.flatMap(getOutputFlatMapFunction(),RowEncoder.apply(structType));
	
		String checkpointDir1 = "file1_sink_checkpoint_" + StreamConstants.RANDOM_NUM;;
		String checkpointDir2 = "file2_sink_checkpoint_" + StreamConstants.RANDOM_NUM;
		


	    try {
		    StreamingQuery query1 = seqs1.writeStream()
		    		.outputMode("append")
		    		.format("text")
		    	    .option("path", output + OUTPUT_FOLDER_NAME_FIRST_PAIR)
		    	    .option("checkpointLocation", checkpointDir1)
		    	    .partitionBy("timestamp")
		    		.start();
		    
		    StreamingQuery query2 = seqs2.writeStream()
		    		.outputMode("append")
		    		.format("text")
		    	    .option("path", output + OUTPUT_FOLDER_NAME_SECOND_PAIR)
		    	    .option("checkpointLocation", checkpointDir2)
		    	    .partitionBy("timestamp")
		    		.start();
		    
	    	ControlFinishReader control = ControlFinishReader.getControl();
			
	    	while (query1.isActive() && query2.isActive()) {
	    		if (!query1.status().isDataAvailable() && !query2.status().isDataAvailable() 
	    				&& !query1.status().isTriggerActive() 
	    				&& !query2.status().isTriggerActive()
	    				&& !query1.status().message().equals("Initializing sources") 
	    				&& !query2.status().message().equals("Initializing sources")
	    				&& control.isFlag()) {
	    			
	    			query1.stop();
	    			query2.stop();

	    			FileSystem fs = FileSystem.get(new Configuration());
	    			writeSuccessFile(fs,output + OUTPUT_FOLDER_NAME_FIRST_PAIR);
	    			writeSuccessFile(fs,output + OUTPUT_FOLDER_NAME_SECOND_PAIR);
	    			
	    			deleteSparkMetadata(fs,output + OUTPUT_FOLDER_NAME_FIRST_PAIR);
	    			deleteSparkMetadata(fs,output + OUTPUT_FOLDER_NAME_SECOND_PAIR);
	    			
	    			deleteExtraFilesPaired(StreamConstants.INPUT_DIR,checkpointDir1,checkpointDir2);
	    		}		
	    		query1.awaitTermination(5000);
	    		query2.awaitTermination(5000);
	    	}	
			
		} catch (StreamingQueryException | TimeoutException e) {
			System.out.print(" --- Streaming failure \n --- ");
			e.printStackTrace();
		}	
	    
	}	
	
	//// Auxiliary functions
	
	// Comparators
	/**
	 * Comparator to compare directory names with the format '<label>=<int>-<int>', e.g: timestamp=0-1.
	 */
	private static Comparator<FileStatus> getFolderComparator() {
		return new Comparator<FileStatus>() {
			
			@Override
			public int compare(FileStatus f1, FileStatus f2) {
				String[] t1 = f1.getPath().getName().split("=")[1].split("-");
				String[] t2 = f2.getPath().getName().split("=")[1].split("-");
				
				int iterationsCompared = Integer.compare(Integer.parseInt(t1[0]),Integer.parseInt(t2[0]));
				if (iterationsCompared == 0) {
					return Integer.compare(Integer.parseInt(t1[1]),Integer.parseInt(t2[1]));
				}
				return iterationsCompared;
			}
			
		};
	}
	/**
	 * Comparator to compare file names
	 */
	private static Comparator<FileStatus> getFileComparator() {
		return new Comparator<FileStatus>() {
			
			@Override
			public int compare(FileStatus f1, FileStatus f2) {
				return f1.getPath().getName().compareTo(f2.getPath().getName());
			}
			
		};
	}
	
	/**
	 * Get a StructType with sequence and timestamp columns
	 */
	private static StructType getOutputStructType () {
	    StructType structType = new StructType();
	    structType = structType.add("timestamp",DataTypes.StringType, false);
	    structType = structType.add("sequence",DataTypes.StringType, false);
	    
	    return structType;
	}
	
	/**
	 * Get a FlatMapFunction to convert a dataset with sequence and timestamp columns
	 * to dataset with a single row
	 */
	private static FlatMapFunction<SequenceWithTimestamp, Row> getOutputFlatMapFunction () {
		
		return new FlatMapFunction<SequenceWithTimestamp, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
	    	public Iterator<Row> call (SequenceWithTimestamp seq) throws Exception {
	    		String timestamp = seq.getTimestamp();
		    	String sequence = seq.getSequence().toString();
		    	
		    	List<String> data = new ArrayList<>();
		    	data.add(timestamp);
		    	data.add(sequence);
		    	List<Row> list = new ArrayList<>();
		    	list.add(RowFactory.create(data.toArray()));
		    	
		    	return list.iterator();
	    	}
	    };
	}
	
	/**
	 * write _SUCCESS file
	 * 
	 * @param partsFolder the parts folder
	 */
	private static void writeSuccessFile (FileSystem fs, String partsFolder) throws IOException {		

		String successFile = partsFolder + "/_SUCCESS";
		fs.create(new Path(successFile));
	}
	
	/**
	 * delete _spark_metadata folder
	 * 
	 * @param partsFolder the parts folder
	 */
	private static void deleteSparkMetadata (FileSystem fs, String partsFolder) {		

		String metadataFolder = partsFolder + "/_spark_metadata";
		try {
			fs.delete(new Path(metadataFolder),true);
		} catch (IOException e) {
			return;
		}
		
	}
	
	/**
	 * delete the intermediate files that spark processed
	 * and the checkpoint directory
	 * 
	 * @param partsFolder the parts folder
	 */
	private static void deleteExtraFiles(String inputDir, String checkpointDir) {
		try {		
	    	Configuration conf = new Configuration();
	    	FileSystem fs = FileSystem.get(conf);
	    	
			fs.delete(new Path(inputDir), true);
			fs.delete(new Path(checkpointDir), true);
		} catch (IOException e) {
			return;
		}
	}
	
	private static void deleteExtraFilesPaired(String inputDir, String checkpointDir1, String checkpointDir2) {
		try {
	    	Configuration conf = new Configuration();
	    	FileSystem fs = FileSystem.get(conf);
	    	
			fs.delete(new Path(inputDir), true);
			fs.delete(new Path(checkpointDir1), true);
			fs.delete(new Path(checkpointDir2), true);
		} catch (IOException e) {
			return;
		}
	}
}
