package com.oscar.castellanos.sequal.sequalmodel.stream.reader;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FASTAReaderWorker extends Thread{

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
    		String lineAux = null;
    		List<String> basesLines = new ArrayList<>();
    		
    		// skip sequence parts that will read previous thread
    		boolean isLineName = false;
    		
			// we are going to count the chars that will read previous thread
    		long notMyChars = 0;
    		
			line1 = ReaderUtils.readLine(bf);
			while (line1 != null && line1.equals("\n")) {
				notMyChars ++;
				line1 = ReaderUtils.readLine(bf);
			}

    		while(!isLineName && line1 != null) {

    			isLineName = isLineName(line1);
        		
        		if (!isLineName) {
        			notMyChars += line1.length();
        			line1 = ReaderUtils.readLine(bf);
        		}
    		}
    		
    		if (!isLineName) {
    			bw.close();
    			bf.close();
    			fsOutput.delete(auxPath, false);
    			return;
    		} 		

    		lineAux = ReaderUtils.readLine(bf);
    		while (lineAux != null && !isLineName(lineAux)) {
    			basesLines.add(lineAux);
    			lineAux = ReaderUtils.readLine(bf);
    		}
    		
    		this.maxFileBytes = this.maxFileBytes - notMyChars;
    		
    		while (basesLines.size() > 0 && basesLines.get(basesLines.size() - 1).endsWith("\n")) {
    				  
        		bw.write(this.sequencePrefix + line1);
        		this.skippedChars += line1.length();
        		
        		for (String basesLine : basesLines) {
        			bw.write(basesLine);
        			this.skippedChars += basesLine.length();
        		}
        		
        		bw.write(timestamp);	
    				
    			if (this.skippedChars >= maxFileBytes) break;

    			line1 = lineAux;
    			basesLines.clear();
    			
        		if (lineAux != null) lineAux = ReaderUtils.readLine(bf);
        		while (lineAux != null && !isLineName(lineAux)) {
        			basesLines.add(lineAux);
        			lineAux = ReaderUtils.readLine(bf);
        		}
        		
    		}
		
    		bw.flush();
    		bw.close();   		
    		
    		if (fsOutput.getFileStatus(auxPath).getLen() > 0) {
    			fsOutput.rename(auxPath, outputPath);
    		} else fsOutput.delete(auxPath, false);	
    			
    		bf.close();
				
		} catch (IOException e) {}	
	}
	
    private boolean isLineName(String line) {   	
    	return line.startsWith(">");  
    }

}
