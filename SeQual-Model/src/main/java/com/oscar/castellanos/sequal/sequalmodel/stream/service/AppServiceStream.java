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
package com.oscar.castellanos.sequal.sequalmodel.stream.service;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.oscar.castellanos.sequal.sequalmodel.service.AppService;
import com.oscar.castellanos.sequal.sequalmodel.stream.common.SequenceWithTimestamp;
import com.oscar.castellanos.sequal.sequalmodel.stream.dnafilereader.DNAFileReaderFactory;
import com.oscar.castellanos.sequal.sequalmodel.stream.utils.ControlFinishReader;
import com.oscar.castellanos.sequal.sequalmodel.stream.writer.WriterUtils;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class AppServiceStream implements AppService{
	private static final Logger LOGGER = Logger.getLogger(AppServiceStream.class.getName());

	private SparkSession sparkSession;
	
	private String input;
	private String secondInput;
	private String output;
	private String configFile;
	private String masterConf;
	private Level logLevel;

	private boolean isPaired;
	
	private Dataset<SequenceWithTimestamp> sequences;
	
	private FilterService filterService;
	private TrimService trimService;
	private FormatService formatService;
	
	/**
	 * Instantiates a new app service.
	 */
	public AppServiceStream() {
		this.filterService = new FilterService();
		this.trimService = new TrimService();
		this.formatService = new FormatService();
	}

	/**
	 * Configures the Spark app and its context.
	 */
	@Override
	public void start() {

		if (this.logLevel == null) {
			LOGGER.warn("Spark logger level not specified. ERROR level will be used.\n");

			this.setLogLevel(Level.ERROR);
		}
		
	    sparkSession = SparkSession
	    	      .builder()
	    	      .appName("SeQual")
	    	      .getOrCreate();
	    
		if (StringUtils.isBlank(this.masterConf)) {
			LOGGER.warn(
					"Spark master not specified. Using existing conf or \"local[*]\" if the first doesn't exist.\n");

			this.setMasterConf(this.sparkSession.conf().get("spark.master","local[*]"));
		}
		this.sparkSession.conf().set("spark.master", this.masterConf);
		
		LOGGER.info("Starting streaming service \n");		
	}

	/**
	 * Stops the Spark app and its context.
	 */
	@Override
	public void stop() {
		LOGGER.info("Stopping Spark context.\n");
		
		this.sparkSession.close();
		this.sparkSession.stop();
	}
	
	/**
	 * Returns the extension of the specified file.
	 *
	 * @param file Path to the file
	 * @return String extension of the file
	 */
	private String getFormat(String file) {
		return FilenameUtils.getExtension(file);
	}
	
	/**
	 * Gets the file name.
	 *
	 * @param file the file
	 * @return the file name
	 */
	private String getFileName(String file) {
		return FilenameUtils.getBaseName(file);
	}

	/**
	 * Gets the actual format.
	 *
	 * @return the actual format
	 */
	private String getActualFormat() {
		if (this.formatService.formattingFastqToFasta) {
			return "fa";
		}

		return this.getFormat(this.input);
	}
	
	/**
	 * Read.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void read() throws IOException {
				
		ControlFinishReader control = ControlFinishReader.getControl();
		control.setFlag(false);
		
		this.isPaired = StringUtils.isNotBlank(this.secondInput);		
		
		if (isPaired) {
			LOGGER.info("Reading files " + this.input + " and " + this.secondInput + ".\n");
			this.sequences = DNAFileReaderFactory.getPairedReader(this.getFormat(this.input))
					.readFileToDataset(this.input, this.secondInput, this.sparkSession);
		} else {
			LOGGER.info("Reading file " + this.input + ".\n");
			this.sequences = DNAFileReaderFactory.getReader(this.getFormat(this.input))
					.readFileToDataset(this.input, this.sparkSession);
		}	    	    
	}
	
	/**
	 * Write.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void write() throws IOException {
		LOGGER.info("Writing results to " + this.output + ".\n");
		WriterUtils.writeHDFS(this.sequences, this.output, this.isPaired);
		
	}	
	
	/**
	 * Write.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void writeWithSingleFile() throws IOException {
		LOGGER.info("Writing results to " + this.output + ".\n");
		WriterUtils.writeHDFSAndMergeToFile(this.sequences, this.output, 
				this.getFileName(this.input),this.isPaired, this.getActualFormat());	

	}
	
	/**
	 * Filter.
	 */
	@Override
	public void filter() {
		LOGGER.info("Starting filtering.\n");
		
		String format = this.getFormat(input);
		boolean hasQuality = format.compareToIgnoreCase("fq") ==  0 || format.compareToIgnoreCase("fastq") == 0;
		
		this.sequences = this.filterService.filter(this.sequences,this.isPaired,hasQuality);
						
	}

	/**
	 * Format.
	 */
	@Override
	public void format() {
		LOGGER.info("Starting formatting.\n");

		this.sequences = this.formatService.format(this.sequences,this.isPaired);
	}

	/**
	 * Trim.
	 */
	@Override
	public void trim() {
		LOGGER.info("Starting trimming.\n");

		String format = this.getFormat(input);
		boolean hasQuality = format.compareToIgnoreCase("fq") ==  0 || format.compareToIgnoreCase("fastq") == 0;
		
		this.sequences = this.trimService.trim(this.sequences,this.isPaired,hasQuality);
	}

	/**
	 * Measure.
	 *
	 * @param isFirst the is first
	 */
	@Override
	public void measure(Boolean isFirst) {
		// TODO
		/*LOGGER.info("Starting measuring.\n");

		if (!this.sequences.isEmpty()) {
			this.statService.measure(this.sequences, isFirst);
		}*/
	}

	/**
	 * Prints the stats.
	 */
	@Override
	public void printStats() {
		// TODO
		//LOGGER.info("Printing stats.\n");
		//System.out.println(this.statService.getResultsAsString());
	}
	
	/**
	 * Generate file.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@Override
	public void generateConfigFile() throws IOException {

		LOGGER.info("Generating configuration file at location" + this.output
				+ " named as ExecutionParameters.properties.\n");

		InputStream in = this.getClass().getResourceAsStream("/ExecutionParameters.properties");

		byte[] buffer = new byte[in.available()];
		in.read(buffer);

		File targetFile = new File(this.output + "/ExecutionParameters.properties");
		OutputStream outStream = new FileOutputStream(targetFile);
		outStream.write(buffer);
		outStream.close();
		in.close();
	}
	
	/**
	 * Gets the spark log level.
	 *
	 * @return the spark log level
	 */
	@Override
	public Level getLogLevel() {
		return this.logLevel;
	}
	
	/**
	 * Sets the log level.
	 *
	 * @param sparkLogLevel the new log level
	 */
	@Override
	public void setLogLevel(Level logLevel) {
		LOGGER.info("Setting log level " + logLevel + " to Spark and dependencies loggers.\n");

		this.logLevel = logLevel;

		org.apache.logging.log4j.core.config.Configurator.setLevel("es.udc.gac.hadoop",
				org.apache.logging.log4j.Level.getLevel(logLevel.toString()));

		Logger.getLogger("io.netty").setLevel(this.logLevel);
		Logger.getLogger("org").setLevel(logLevel);
		Logger.getLogger("akka").setLevel(logLevel);
	}
	
	/**
	 * Gets the parameter.
	 *
	 * @param param the param
	 * @return the parameter
	 */
	@Override
	public String getParameter(String param) {
		return ExecutionParametersManager.getParameter(param);
	}

	/**
	 * Sets a value for the specified parameter.
	 *
	 * @param param specified parameter
	 * @param value specified value for the parameter
	 */
	@Override
	public void setParameter(String param, String value) {
		ExecutionParametersManager.setParameter(param, value);
	}
	
	/**
	 * Gets the spark conf.
	 *
	 * @return the spark conf
	 */
	public SparkConf getSparkConf() {
		return this.sparkSession.sparkContext().conf();
	}
	
	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	@Override
	public String getInput() {
		return this.input;
	}
	
	/**
	 * Sets the input.
	 *
	 * @param input the new input
	 */
	@Override
	public void setInput(String input) {
		this.input = input;
	}
	
	/**
	 * Gets the second input.
	 *
	 * @return the second input
	 */
	@Override
	public String getSecondInput() {
		return this.secondInput;
	}
	
	/**
	 * Sets the second input.
	 *
	 * @param secondInput the new second input
	 */
	@Override
	public void setSecondInput(String secondInput) {
		this.secondInput = secondInput;
	}
	
	/**
	 * Gets the output.
	 *
	 * @return the output
	 */
	@Override
	public String getOutput() {
		return this.output;
	}
	
	/**
	 * Sets the output.
	 *
	 * @param output the new output
	 */
	@Override
	public void setOutput(String output) {
		this.output = output;
	}
	
	/**
	 * Gets the config file.
	 *
	 * @return the config file
	 */
	@Override
	public String getConfigFile() {
		return this.configFile;
	}
	
	/**
	 * Sets the config file.
	 *
	 * @param configFile the new config file
	 */
	@Override
	public void setConfigFile(String configFile) {
		this.configFile = configFile;
		ExecutionParametersManager.setConfigFile(this.configFile);
	}
	
	/**
	 * Gets the master conf.
	 *
	 * @return the master conf
	 */
	@Override
	public String getMasterConf() {
		return this.masterConf;
	}
	
	/**
	 * Sets the master conf.
	 *
	 * @param masterConf the new master conf
	 */
	@Override
	public void setMasterConf(String masterConf) {
		LOGGER.info("Setting Spark master configuration as " + masterConf + ".\n");

		this.masterConf = masterConf;
	}
	
	/**
	 * Gets the spark session.
	 *
	 * @return the spark session
	 */
	public SparkSession getSparkSession() {
		return this.sparkSession;
	}

	/**
	 * Sets the spark session.
	 *
	 * @param sparkSession the new spark session
	 */
	public void setSparkSession(SparkSession sparkSession) {
		this.sparkSession = sparkSession;		
	}	
	
}
