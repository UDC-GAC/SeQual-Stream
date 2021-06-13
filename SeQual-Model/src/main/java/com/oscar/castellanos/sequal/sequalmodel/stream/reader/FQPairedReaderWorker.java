package com.oscar.castellanos.sequal.sequalmodel.stream.reader;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FQPairedReaderWorker extends Thread{

	int iteration;
	int num_worker;
	long maxFileBytes;
	BufferedInputStream bf1;
	BufferedInputStream bf2;
	FileSystem fsOutput;
	String tmpDir;
	String outputDir;
	String sequencePairPrefix;
	volatile long skippedChars1 = 0;
	volatile long skippedChars2 = 0;
	
	public void startRead(int iteration, int num_worker, long maxFileBytes, 
			BufferedInputStream bf1, BufferedInputStream bf2, FileSystem fsOutput, 
			String tmpDir, String outputDir, String sequencePairPrefix) {
		
		this.iteration = iteration;
		this.num_worker = num_worker;
		this.maxFileBytes = maxFileBytes;
		this.bf1 = bf1;
		this.bf2 = bf2;
		this.tmpDir = tmpDir;
		this.outputDir = outputDir;
		this.fsOutput = fsOutput;
		this.sequencePairPrefix = sequencePairPrefix;
		
		this.start();
	}
	
	public long getSkippedChars1() {
		return skippedChars1;
	}
	
	public long getSkippedChars2() {
		return skippedChars2;
	}
	
	public void run() {  

		Path auxPath = new Path(this.tmpDir + "/inputPart-" + iteration + "-" + num_worker);
		Path outputPath = new Path(this.outputDir + "/inputPart-" + iteration + "-" + num_worker);
					     
		short replication = 1;
		
		try {
				
			String timestamp = String.valueOf(this.iteration) + "-" + this.num_worker + "\n";
			
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsOutput.create(auxPath,replication)));
		
			String line1_1 = null;
			String line1_2 = null;
			String line1_3 = null;
			String line1_4 = null;
			
			String line2_1 = null;
			String line2_2 = null;
			String line2_3 = null;
			String line2_4 = null;

    		// skip sequence parts that will read previous thread
    		boolean newFullSequence1 = false;
    		boolean isFirstLineName1 = false;
    		boolean isNextLineName1 = false;
    		
    		boolean newFullSequence2 = false;
    		boolean isFirstLineName2 = false;
    		boolean isNextLineName2 = false;
    		
			// we are going to count the chars that will read previous thread
			long notMyChars1 = 0;
			long notMyChars2 = 0;
    		
    		line1_1 = ReaderUtils.readLine(bf1);
			while (line1_1 != null && line1_1.equals("\n")) {
				notMyChars1 ++;
				line1_1 = ReaderUtils.readLine(bf1);
			}
			if (line1_1 != null) line1_2 = ReaderUtils.readLine(bf1);
			
    		while(!newFullSequence1 && line1_2 != null) {

    			isFirstLineName1 = isFirstLineName(line1_1,line1_2);
    			isNextLineName1 = isNextLineName(line1_1,line1_2);
        		
        		newFullSequence1 = isFirstLineName1 || isNextLineName1;
        		if (!newFullSequence1) {
        			notMyChars1 += line1_1.length() + line1_2.length();
        			line1_1 = ReaderUtils.readLine(bf1);
        			if (line1_1 != null) line1_2 = ReaderUtils.readLine(bf1); else line1_2 = null;
        		}
    		}
    		
    		line2_1 = ReaderUtils.readLine(bf2);
			while (line2_1 != null && line2_1.equals("\n")) {
				notMyChars2 ++;
				line2_1 = ReaderUtils.readLine(bf2);
			}
			if (line2_1 != null) line2_2 = ReaderUtils.readLine(bf2);

    		while(!newFullSequence2 && line2_2 != null) {

    			isFirstLineName2 = isFirstLineName(line2_1,line2_2);
    			isNextLineName2 = isNextLineName(line2_1,line2_2);
        		
    			newFullSequence2 = isFirstLineName2 || isNextLineName2;
        		if (!newFullSequence2) {
        			notMyChars2 += line2_1.length() + line2_2.length();
        			line2_1 = ReaderUtils.readLine(bf2);
        			if (line2_1 != null) line2_2 = ReaderUtils.readLine(bf2); else line2_2 = null;
        		}
    		}
    		
    		if (!newFullSequence1 || !newFullSequence2) {
    			bw.close();
    			bf1.close();
    			bf2.close();
    			fsOutput.delete(auxPath, false);
    			return;
    		}
    		
    		if (isFirstLineName1 && isFirstLineName2) {
    			line1_3 = ReaderUtils.readLine(bf1);
        		if (line1_3 != null) line1_4 = ReaderUtils.readLine(bf1);
        		
    			line2_3 = ReaderUtils.readLine(bf2);
        		if (line2_3 != null) line2_4 = ReaderUtils.readLine(bf2);
        		
    		} else if (isNextLineName1 && isNextLineName2) {
    			notMyChars1 += line1_1.length();
    			line1_1 = line1_2;
    			line1_2 = ReaderUtils.readLine(bf1);
    			if (line1_2 != null) line1_3 = ReaderUtils.readLine(bf1);
        		if (line1_3 != null) line1_4 = ReaderUtils.readLine(bf1);
        		
        		notMyChars2 += line2_1.length();
    			line2_1 = line2_2;
    			line2_2 = ReaderUtils.readLine(bf2);
        		if (line2_2 != null) line2_3 = ReaderUtils.readLine(bf2);
        		if (line2_3 != null) line2_4 = ReaderUtils.readLine(bf2);
    		} else  {
    			// this shouldn't happen since input files need to have same size
    			bw.close();
    			bf1.close();
    			bf2.close();
    			fsOutput.delete(auxPath, false);
    			return;
    		}
    		this.maxFileBytes = this.maxFileBytes - notMyChars1 - notMyChars2;
    			
			while (line1_4 != null && line1_4.endsWith("\n") && line2_4 != null  && line2_4.endsWith("\n")) { 
	  
				bw.write(this.sequencePairPrefix + line1_1);
				bw.write(line2_1);
				bw.write(line1_2);
				bw.write(line2_2); 
				bw.write(line1_3); 
				bw.write(line2_3); 
				bw.write(line1_4);
				bw.write(line2_4);
				bw.write(timestamp); 
      				
    			// using my readLine
    			this.skippedChars1 += line1_1.length() + line1_2.length() + line1_3.length() + line1_4.length();
    			this.skippedChars2 += line2_1.length() + line2_2.length() + line2_3.length() + line2_4.length();
    			
    			if (this.skippedChars1 + this.skippedChars2 > maxFileBytes) break;

        		line1_1 = ReaderUtils.readLine(bf1);
        		if (line1_1 != null) line1_2 = ReaderUtils.readLine(bf1); else line1_2 = null;
        		if (line1_2 != null) line1_3 = ReaderUtils.readLine(bf1); else line1_3 = null;
        		if (line1_3 != null) line1_4 = ReaderUtils.readLine(bf1); else line1_4 = null;
        		
          		line2_1 = ReaderUtils.readLine(bf2);
        		if (line2_1 != null) line2_2 = ReaderUtils.readLine(bf2); else line2_2 = null;
        		if (line2_2 != null) line2_3 = ReaderUtils.readLine(bf2); else line2_3 = null;
        		if (line2_3 != null) line2_4 = ReaderUtils.readLine(bf2); else line2_4 = null;
    		}

    		bw.flush();
    		bw.close();
    			
    		if (fsOutput.getFileStatus(auxPath).getLen() > 0) {
    			fsOutput.rename(auxPath, outputPath);
    		} else fsOutput.delete(auxPath, false);	
    			
    		bf1.close();
    		bf2.close();
    		
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
