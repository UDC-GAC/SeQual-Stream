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

public class FQReaderUtils extends ReaderUtils {

    String inputFile;
    FileSystem fsInput;
    
    /** A prefix to differentiate every sequence. */
    public String SEQUENCE_NAME_PREFIX = "@STREAM_GEN_SEQ";
        
	/**
	 * Start thread to read input file (even if it is being downloaded) 
	 * and write fragments on outputDir where spark streaming is reading.
	 *
	 * @param file the input path
	 * @param outputDir the directory where spark streaming is waiting files
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
    public void readAndWriteDirectory(String file, String outputDir, SparkSession sparkSession) throws IOException {
    	this.inputFile = file;
    	this.outputDir = outputDir;
    	this.sparkSession = sparkSession;
    	
    	Configuration conf = new Configuration();
    	fsOutput = FileSystem.get(conf);
    	fsOutput.mkdirs(new Path(outputDir));
    	
    	Path inPath = new Path(file);
    	fsInput = inPath.getFileSystem(conf);
    	
        this.start();
    }
       
    public void run() {
	   	
    	try {
		
    		this.setupReader();
    					
       		long start = System.currentTimeMillis();
       		long end = start + timeoutTime;
    		int iteration = 0;
    		long skipChars = 0;
    		long fileBytes = 0;
    		
    		fsOutput.mkdirs(new Path(this.tmpDir));
    		
    		int num_readers = this.maxReaderThreads;
    		long partition_skip = maxFileBytes;

    		while(true) {
		
        		List<FQReaderWorker> workerList = new ArrayList<>();
        		
        		for (int i = 0; i < num_readers; i++) {
        			FQReaderWorker worker = new FQReaderWorker();
        			FSDataInputStream fsInputThread = fsInput.open(new Path(inputFile));
        			
        			try {
        				fsInputThread.seek(skipChars + partition_skip*i);

            			worker.startRead(iteration, i, partition_skip, new BufferedInputStream(fsInputThread), 
            					fsOutput, this.tmpDir, this.outputDir, SEQUENCE_NAME_PREFIX);
            			workerList.add(worker);
            			
        			} catch (IOException e) {
        				// seek failed
            			break;
        			}
        		}
    			
        		// wait for all threads
        		for (FQReaderWorker worker: workerList) {
        			worker.join();
        		}
    			 
        		// bytes read from all threads
        		for (FQReaderWorker worker: workerList) {
        			skipChars += worker.getSkippedChars();
        			fileBytes += worker.getSkippedChars();
        		}
        		
    			if (fileBytes > 0) {
    				start = System.currentTimeMillis();
    				end = start + this.timeoutTime;	
    			}
    			
    			if (System.currentTimeMillis() > end) break;
    			
    			if (fileBytes <= partition_skip*num_readers) {
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
