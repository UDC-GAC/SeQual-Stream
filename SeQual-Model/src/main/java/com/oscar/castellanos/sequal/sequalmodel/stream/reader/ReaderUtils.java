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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import com.oscar.castellanos.sequal.sequalmodel.stream.utils.ControlFinishReader;
import com.oscar.castellanos.sequal.sequalmodel.stream.utils.StreamConstants;

public abstract class ReaderUtils extends Thread {

    String outputDir;
    String tmpDir = "tmp-" + StreamConstants.RANDOM_NUM;
    FileSystem fsOutput;
    SparkSession sparkSession;
    long sleepTime = 5000;
    long timeoutTime = sleepTime * 3;
    long maxFileBytes;
    long minFileBytes;
    long minTotalBytes;
    long hdfsBlockSize;
    int paralellism;
    int maxReaderThreads;
    
    // while the control flag is false, reading has not ended
    ControlFinishReader control = ControlFinishReader.getControl();

	/**
	 * Read a line of a file, just like the bufferedReader.readline() method 
	 * but including the final line feed if exists
	 *
	 * @param reader the BufferedInputStream
	 * @return the output line
	 */
    static String readLine(BufferedInputStream reader) {
    	String readString;
    	StringBuilder readStr = new StringBuilder();
    	
    	try {
        	int c = reader.read();
        	while(c != -1 && (char) c != '\n') {
        		readStr.append((char)c);
        		c = reader.read();
        	}	

        	if (c != -1) readStr.append((char)c);
        	
    	} catch (Exception e) {
    		throw new RuntimeException(e);
    	}

    	readString = readStr.toString();
    	if (readString.equals("")) return null;
    	else return readString;
    	
    }

	/**
	 * Deletes the the temporal directory and sets the control flag to true
	 * to indicate that read has finished
	 *
	 */
    void finishReader(){
    	this.control.setFlag(true);
		try {
			fsOutput.delete(new Path(tmpDir), true);
		} catch (IOException e) {}
    }
    
	/**
	 * Tries to open the input file
	 * throws IOException if not possible
	 *
	 * @param inFile the input File
	 * @throws IOException
	 */
    public void tryOpenFile(String inFile) throws IOException {
	    Configuration conf = new Configuration();
    	Path inPath = new Path(inFile);
    	FileSystem fsInput = inPath.getFileSystem(conf);
	    
    	fsInput.open(inPath).close(); 	
    }

	/**
	 * Tries to open the input file and its pair
	 * throws IOException if not possible
	 *
	 * @param inFile1 the input File
	 * @param inFile2 the paired input File
	 * @throws IOException
	 */
    public void tryOpenFilePaired(String inFile1, String inFile2) throws IOException {
	    Configuration conf = new Configuration();
    	Path inPath1 = new Path(inFile1);
    	FileSystem fsInput1 = inPath1.getFileSystem(conf);
    	
    	Path inPath2 = new Path(inFile2);
    	FileSystem fsInput2 = inPath2.getFileSystem(conf);
    	
    	fsInput1.open(inPath1).close(); 	
	    fsInput2.open(inPath2).close(); 
    }
    
    public void setupReader() throws InterruptedException {
		/* we need to get from spark conf the number of executor cores:
		 * spark.default.parallelism
		 * 
		 * from java the the number of threads:
		 * java.lang.Runtime.getRuntime().availableProcessors()
		 * 
		 * from hdfs the blocksize:
		 * conf.get("dfs.blocksize")
		 */
		
		this.maxReaderThreads = java.lang.Runtime.getRuntime().availableProcessors() - 2;
		if (this.maxReaderThreads < 1) this.maxReaderThreads = 1;

		try {
			this.paralellism = Integer.parseInt(sparkSession.sparkContext().getConf().get("spark.default.parallelism"));
			this.hdfsBlockSize = Long.parseLong(new Configuration().get("dfs.blocksize"));

		} catch (Exception e) {
			System.out.println("Missing HDFS config, using default values");
			this.hdfsBlockSize = 128 * 1024 * 1024;
			this.paralellism = 1;
		}
		
		this.minFileBytes = this.hdfsBlockSize;
		this.minTotalBytes = this.hdfsBlockSize * this.paralellism;
		this.maxFileBytes = this.minTotalBytes / this.maxReaderThreads;
		
		if (this.maxFileBytes < this.minFileBytes) {
			this.maxFileBytes = this.minFileBytes;
			this.maxReaderThreads = (int) Math.ceil((double) this.minTotalBytes / this.maxFileBytes);
		}		
    }
    
}
