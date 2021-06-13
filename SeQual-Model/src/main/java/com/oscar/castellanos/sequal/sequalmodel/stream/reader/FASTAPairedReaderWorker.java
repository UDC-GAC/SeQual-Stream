package com.oscar.castellanos.sequal.sequalmodel.stream.reader;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FASTAPairedReaderWorker  extends Thread{

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
    		String lineAux1 = null;
    		List<String> basesLines1 = new ArrayList<>();
			
			String line2_1 = null;
    		String lineAux2 = null;
    		List<String> basesLines2 = new ArrayList<>();

    		// skip sequence parts that will read previous thread
    		boolean isLineName1 = false;
    		boolean isLineName2 = false;
    		
			// we are going to count the chars that will read previous thread
			long notMyChars1 = 0;
			long notMyChars2 = 0;
    		
    		line1_1 = ReaderUtils.readLine(bf1);
			while (line1_1 != null && line1_1.equals("\n")) {
				notMyChars1 ++;
				line1_1 = ReaderUtils.readLine(bf1);
			}
			
    		while(!isLineName1 && line1_1 != null) {

    			isLineName1 = isLineName(line1_1);

        		if (!isLineName1) {
        			notMyChars1 += line1_1.length();
        			line1_1 = ReaderUtils.readLine(bf1);
        		}
    		}
    		
    		line2_1 = ReaderUtils.readLine(bf2);
			while (line2_1 != null && line2_1.equals("\n")) {
				notMyChars2 ++;
				line2_1 = ReaderUtils.readLine(bf2);
			}

    		while(!isLineName2 && line2_1 != null) {

    			isLineName2 = isLineName(line2_1);
    			
        		if (!isLineName2) {
        			notMyChars2 += line2_1.length();
        			line2_1 = ReaderUtils.readLine(bf2);
        		}
    		}
    		
    		if (!isLineName1 || !isLineName2) {
    			bw.close();
    			bf1.close();
    			bf2.close();
    			fsOutput.delete(auxPath, false);
    			return;
    		}
    		
    		if (isLineName1 && isLineName2) {
    			lineAux1 = ReaderUtils.readLine(bf1);
        		while (lineAux1 != null && !isLineName(lineAux1)) {
        			basesLines1.add(lineAux1);
        			lineAux1 = ReaderUtils.readLine(bf1);
        		}
        		
        		lineAux2 = ReaderUtils.readLine(bf2);
        		while (lineAux2 != null && !isLineName(lineAux2)) {
        			basesLines2.add(lineAux2);
        			lineAux2 = ReaderUtils.readLine(bf2);
        		}
    		} else  {
    			// this shouldn't happen since input files need to have same size
    			bw.close();
    			bf1.close();
    			bf2.close();
    			fsOutput.delete(auxPath, false);
    			return;
    		}
    		this.maxFileBytes = this.maxFileBytes - notMyChars1 - notMyChars2;
  			
    		while (basesLines1.size() > 0 && basesLines1.get(basesLines1.size() - 1).endsWith("\n")
    				&& basesLines2.size() > 0 && basesLines2.get(basesLines2.size() - 1).endsWith("\n")) {
	  
				bw.write(this.sequencePairPrefix + line1_1);
				this.skippedChars1 += line1_1.length();
				
        		for (String basesLine1 : basesLines1) {
        			bw.write(basesLine1);
        			this.skippedChars1 += basesLine1.length();
        		}
        				
				bw.write(line2_1);
				this.skippedChars2 += line2_1.length();

           		for (String basesLine2 : basesLines2) {
        			bw.write(basesLine2);
        			this.skippedChars2 += basesLine2.length();
        		}
				bw.write(timestamp); 
    			
    			if (this.skippedChars1 + this.skippedChars2 > maxFileBytes) break;

    			line1_1 = lineAux1;
    			basesLines1.clear();
    			
        		if (lineAux1 != null) lineAux1 = ReaderUtils.readLine(bf1);
        		while (lineAux1 != null && !isLineName(lineAux1)) {
        			basesLines1.add(lineAux1);
        			lineAux1 = ReaderUtils.readLine(bf1);
        		}
        		
    			line2_1 = lineAux2;
    			basesLines2.clear();
    			
        		if (lineAux2 != null) lineAux2 = ReaderUtils.readLine(bf2);
        		while (lineAux2 != null && !isLineName(lineAux2)) {
        			basesLines2.add(lineAux2);
        			lineAux2 = ReaderUtils.readLine(bf2);
        		}
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

    private boolean isLineName(String line) {   	
    	return line.startsWith(">");  
    }
}
