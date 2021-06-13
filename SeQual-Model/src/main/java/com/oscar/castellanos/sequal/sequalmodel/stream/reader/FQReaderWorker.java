package com.oscar.castellanos.sequal.sequalmodel.stream.reader;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FQReaderWorker extends Thread{

	int iteration;
	int num_worker;
	long maxFileBytes;
	BufferedInputStream bf;
	FileSystem fsOutput;
	String tmpDir;
	String outputDir;
	String sequencePrefix;
	volatile long skippedChars = 0;
	
	public void startRead(int iteration, int num_worker, long maxFileBytes, 
			BufferedInputStream bf, FileSystem fsOutput, String tmpDir, String outputDir, String sequencePrefix) {
		
		this.iteration = iteration;
		this.num_worker = num_worker;
		this.maxFileBytes = maxFileBytes;
		this.bf = bf;
		this.tmpDir = tmpDir;
		this.outputDir = outputDir;
		this.fsOutput = fsOutput;
		this.sequencePrefix = sequencePrefix;
		
		this.start();
	}
	
	public long getSkippedChars() {
		return skippedChars;
	}
	
	public void run() {

		Path auxPath = new Path(this.tmpDir + "/inputPart-" + iteration + "-" + num_worker);
		Path outputPath = new Path(this.outputDir + "/inputPart-" + iteration + "-" + num_worker);
					      				
		short replication = 1;
		
		try {
				
			String timestamp = String.valueOf(this.iteration) + "-" + this.num_worker + "\n"; 
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsOutput.create(auxPath,replication)));
		
    		String line1 = null;
    		String line2 = null;
    		String line3 = null;
    		String line4 = null;

    		// skip sequence parts that will read previous thread
    		boolean newFullSequence = false;
    		boolean isFirstLineName = false;
    		boolean isNextLineName = false;
    		
			// we are going to count the chars that will read previous thread
    		long notMyChars = 0;
    		
			line1 = ReaderUtils.readLine(bf);
			while (line1 != null && line1.equals("\n")) {
				notMyChars ++;
				line1 = ReaderUtils.readLine(bf);
			}
			if (line1 != null) line2 = ReaderUtils.readLine(bf);

    		while(!newFullSequence && line2 != null) {

        		isFirstLineName = isFirstLineName(line1,line2);
        		isNextLineName = isNextLineName(line1,line2);
        		
        		newFullSequence = isFirstLineName || isNextLineName;
        		if (!newFullSequence) {
        			notMyChars += line1.length() + line2.length();
        			line1 = ReaderUtils.readLine(bf);
        			if (line1 != null) line2 = ReaderUtils.readLine(bf); else line2 = null;
        		}
    		}
    		
    		if (!newFullSequence) {
    			bw.close();
    			bf.close();
    			fsOutput.delete(auxPath, false);
    			return;
    		} 		
    		
    		if (isFirstLineName) {
    			line3 = ReaderUtils.readLine(bf);
        		if (line3 != null) line4 = ReaderUtils.readLine(bf);
    		} else {
    			notMyChars += line1.length();
    			line1 = line2;
    			line2 = ReaderUtils.readLine(bf);
        		if (line2 != null) line3 = ReaderUtils.readLine(bf);
        		if (line3 != null) line4 = ReaderUtils.readLine(bf);
    		}
    		this.maxFileBytes = this.maxFileBytes - notMyChars;
    		
    		while (line4 != null && line4.endsWith("\n")) {
    				  
        		bw.write(this.sequencePrefix + line1);
        		bw.write(line2); 
        		bw.write(line3); 
        		bw.write(line4);
        		bw.write(timestamp);	
	
    			// using my readLine
    			this.skippedChars += line1.length() + line2.length() + line3.length() + line4.length();
    				
    			if (this.skippedChars >= maxFileBytes) break;

        		line1 = ReaderUtils.readLine(bf);
        		if (line1 != null) line2 = ReaderUtils.readLine(bf); else line2 = null;
        		if (line2 != null) line3 = ReaderUtils.readLine(bf); else line3 = null;
        		if (line3 != null) line4 = ReaderUtils.readLine(bf); else line4 = null;
    		}
		
    		bw.flush();
    		bw.close();   		
    		
    		if (fsOutput.getFileStatus(auxPath).getLen() > 0) {
    			fsOutput.rename(auxPath, outputPath);
    		} else fsOutput.delete(auxPath, false);	
    			
    		bf.close();
				
		} catch (IOException e) {}	
	}
	
    private boolean isFirstLineName(String line, String nextLine) {
	    	
    	if (!line.startsWith("@")) return false;
    	else if (nextLine == null) return false;
    	else if (nextLine.startsWith("@")) return false;
    	else return true;
    	
    }
    
    private boolean isNextLineName(String line, String nextLine) {
    	
    	if (nextLine == null) return false;
    	
    	if (!nextLine.startsWith("@")) return false;
    	else if (line.startsWith("@")) return true;
    	else if (line.equals("+\n")) return false;
    	else return true;
    }
}
