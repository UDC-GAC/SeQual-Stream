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
package com.oscar.castellanos.sequal.sequalmodel.stream.reader;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

public class FQPairedReaderUtils extends ReaderUtils {

    String inputFile1;
    String inputFile2;
    FileSystem fsInput1;
    FileSystem fsInput2;
    
    /** A prefix to differentiate every paired sequence. */
    public String SEQUENCE_PAIR_NAME_PREFIX = "@STREAM_GEN_PAIR_SEQ";
    
	/**
	 * Start thread to read inputs file (even if they are being downloaded) 
	 * and write fragments on outputDir where spark streaming is reading.
	 *
	 * @param file1 the input1 path
	 * @param file2 the input2 path
	 * @param outputDir the directory where spark streaming is waiting files
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
    public void readAndWriteDirectory(String file1, String file2, String outputDir, SparkSession sparkSession) throws IOException {
    	this.inputFile1 = file1;
    	this.inputFile2 = file2;
    	this.outputDir = outputDir;
    	this.sparkSession = sparkSession;
    	
    	Configuration conf = new Configuration();
    	fsOutput = FileSystem.get(conf);
    	fsOutput.mkdirs(new Path(outputDir));
    	
    	Path inPath1 = new Path(file1);
    	fsInput1 = inPath1.getFileSystem(conf);
    	Path inPath2 = new Path(file2);
    	fsInput2 = inPath2.getFileSystem(conf);
    	
        this.start();
    }  
    
    public void run() {
	   	
    	try {
		
    		this.setupReader();
    					
       		long start = System.currentTimeMillis();
       		long end = start + timeoutTime;
    		int iteration = 0;
    		long skipChars1 = 0;
    		long skipChars2 = 0;
    		long fileBytes = 0;
    		
    		fsOutput.mkdirs(new Path(this.tmpDir));
    		
    		int num_readers = this.maxReaderThreads;
    		long partition_skip = maxFileBytes/2;

    		while(true) {
		
        		List<FQPairedReaderWorker> workerList = new ArrayList<>();
        		
        		for (int i = 0; i < num_readers; i++) {
        			FQPairedReaderWorker worker = new FQPairedReaderWorker();
        			FSDataInputStream fsInputThread1 = fsInput1.open(new Path(inputFile1));
        			FSDataInputStream fsInputThread2 = fsInput2.open(new Path(inputFile2));
        			
        			try {
        				fsInputThread1.seek(skipChars1 + partition_skip*i);
        				fsInputThread2.seek(skipChars2 + partition_skip*i);
        				
            			worker.startRead(iteration, i, partition_skip*2, new BufferedInputStream(fsInputThread1), 
            					new BufferedInputStream(fsInputThread2),fsOutput, this.tmpDir, this.outputDir, 
            					this.SEQUENCE_PAIR_NAME_PREFIX);
            			workerList.add(worker);
            			
        			} catch (IOException e) {
        				// seek failed
            			break;
        			}
        		}
    			
        		// wait for all threads
        		for (FQPairedReaderWorker worker: workerList) {
        			worker.join();
        		}
    			 
        		// bytes read from all threads
        		for (FQPairedReaderWorker worker: workerList) {
        			skipChars1 += worker.getSkippedChars1();
        			skipChars2 += worker.getSkippedChars2();
        			fileBytes += worker.getSkippedChars1() + worker.getSkippedChars2();;
        		}
        		
    			if (fileBytes > 0) {
    				start = System.currentTimeMillis();
    				end = start + this.timeoutTime;	
    			}
    			
    			if (System.currentTimeMillis() > end) break;
    			
    			if (fileBytes <= partition_skip*2*num_readers) {
    				if (fileBytes == 0) Thread.sleep(this.sleepTime);
    				else Thread.sleep(this.sleepTime*4);		
    			} 
    			fileBytes = 0;
    			iteration++;
    		}
    		
    		this.finishReader();		
    		
    	} catch (Exception e) {
    		this.finishReader();
    		throw new RuntimeException(e);
    	}
    }
}
