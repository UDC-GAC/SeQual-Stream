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
package com.roi.galegot.sequal.sequalmodel.service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.roi.galegot.sequal.sequalmodel.common.Sequence;
import com.roi.galegot.sequal.sequalmodel.dnafilereader.DNAFileReaderFactory;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;
import com.roi.galegot.sequal.sequalmodel.writer.WriterUtils;

/**
 * The Class AppService.
 */
public class AppService {

	private static final Logger LOGGER = Logger.getLogger(AppService.class.getName());

	private SparkConf sparkConf;
	private JavaSparkContext sparkContext;

	private String input;
	private String secondInput;
	private String output;
	private String configFile;
	private String masterConf;
	private Level logLevel;

	private JavaRDD<Sequence> sequences;

	private FilterService filterService;
	private TrimService trimService;
	private StatService statService;
	private FormatService formatService;

	/**
	 * Instantiates a new app service.
	 */
	public AppService() {
		this.filterService = new FilterService();
		this.trimService = new TrimService();
		this.statService = new StatService();
		this.formatService = new FormatService();
	}

	/**
	 * Configures the Spark app and its context.
	 */
	public void start() {

		if (this.logLevel == null) {
			LOGGER.warn("Spark logger level not specified. ERROR level will be used.\n");

			this.setLogLevel(Level.ERROR);
		}

		this.sparkConf = new SparkConf().setAppName("SeQual");

		if (StringUtils.isBlank(this.masterConf)) {
			LOGGER.warn(
					"Spark master not specified. Using existing conf or \"local[*]\" if the first doesn't exist.\n");

			this.setMasterConf(this.sparkConf.get("spark.master", "local[*]"));
		}

		this.sparkConf.setMaster(this.masterConf);

		this.sparkContext = new JavaSparkContext(this.sparkConf);
	}

	/**
	 * Stops the Spark app and its context.
	 */
	public void stop() {
		LOGGER.info("Stopping Spark context.\n");

		this.sparkContext.close();
		this.sparkContext.stop();
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
		if (this.sequences.first().getHasQuality()) {
			return "fq";
		}

		return "fa";
	}

	/**
	 * Read.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void read() throws IOException {
		if (StringUtils.isNotBlank(this.secondInput)) {
			LOGGER.info("Reading files " + this.input + " and " + this.secondInput + ".\n");
			this.sequences = DNAFileReaderFactory.getPairedReader(this.getFormat(this.input)).readFileToRDD(this.input,
					this.secondInput, this.sparkContext);
		} else {
			LOGGER.info("Reading file " + this.input + ".\n");
			this.sequences = DNAFileReaderFactory.getReader(this.getFormat(this.input)).readFileToRDD(this.input,
					this.sparkContext);
		}

	}

	/**
	 * Write.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void write() throws IOException {
		LOGGER.info("Writing results to " + this.output + ".\n");
		WriterUtils.writeHDFS(this.sequences, this.output);
	}

	/**
	 * Write.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writeWithSingleFile() throws IOException {
		LOGGER.info("Writing results to " + this.output + ".\n");
		WriterUtils.writeHDFSAndMergeToFile(this.sequences, this.output, this.getFileName(this.input),
				this.getActualFormat());
	}

	/**
	 * Filter.
	 */
	public void filter() {
		LOGGER.info("Starting filtering.\n");

		if (!this.sequences.isEmpty()) {
			this.sequences = this.filterService.filter(this.sequences);
		}
	}

	/**
	 * Format.
	 */
	public void format() {
		LOGGER.info("Starting formatting.\n");

		if (!this.sequences.isEmpty()) {
			this.sequences = this.formatService.format(this.sequences);
		}
	}

	/**
	 * Trim.
	 */
	public void trim() {
		LOGGER.info("Starting trimming.\n");

		if (!this.sequences.isEmpty()) {
			this.sequences = this.trimService.trim(this.sequences);
		}
	}

	/**
	 * Measure.
	 *
	 * @param isFirst the is first
	 */
	public void measure(Boolean isFirst) {
		LOGGER.info("Starting measuring.\n");

		if (!this.sequences.isEmpty()) {
			this.statService.measure(this.sequences, isFirst);
		}
	}

	/**
	 * Prints the stats.
	 */
	public void printStats() {
		LOGGER.info("Printing stats.\n");
		System.out.println(this.statService.getResultsAsString());
	}

	/**
	 * Generate file.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
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
	 * Sets the log level.
	 *
	 * @param sparkLogLevel the new log level
	 */
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
	public String getParameter(String param) {
		return ExecutionParametersManager.getParameter(param);
	}

	/**
	 * Sets a value for the specified parameter.
	 *
	 * @param param specified parameter
	 * @param value specified value for the parameter
	 */
	public void setParameter(String param, String value) {
		ExecutionParametersManager.setParameter(param, value);
	}

	/**
	 * Gets the spark conf.
	 *
	 * @return the spark conf
	 */
	public SparkConf getSparkConf() {
		return this.sparkConf;
	}

	/**
	 * Sets the spark conf.
	 *
	 * @param sparkConf the new spark conf
	 */
	public void setSparkConf(SparkConf sparkConf) {
		this.sparkConf = sparkConf;
	}

	/**
	 * Gets the spark context.
	 *
	 * @return the spark context
	 */
	public JavaSparkContext getSparkContext() {
		return this.sparkContext;
	}

	/**
	 * Sets the spark context.
	 *
	 * @param sparkContext the new spark context
	 */
	public void setSparkContext(JavaSparkContext sparkContext) {
		this.sparkContext = sparkContext;
	}

	/**
	 * Gets the input.
	 *
	 * @return the input
	 */
	public String getInput() {
		return this.input;
	}

	/**
	 * Sets the input.
	 *
	 * @param input the new input
	 */
	public void setInput(String input) {
		this.input = input;
	}

	/**
	 * Gets the second input.
	 *
	 * @return the second input
	 */
	public String getSecondInput() {
		return this.secondInput;
	}

	/**
	 * Sets the second input.
	 *
	 * @param secondInput the new second input
	 */
	public void setSecondInput(String secondInput) {
		this.secondInput = secondInput;
	}

	/**
	 * Gets the output.
	 *
	 * @return the output
	 */
	public String getOutput() {
		return this.output;
	}

	/**
	 * Sets the output.
	 *
	 * @param output the new output
	 */
	public void setOutput(String output) {
		this.output = output;
	}

	/**
	 * Gets the config file.
	 *
	 * @return the config file
	 */
	public String getConfigFile() {
		return this.configFile;
	}

	/**
	 * Sets the config file.
	 *
	 * @param configFile the new config file
	 */
	public void setConfigFile(String configFile) {
		this.configFile = configFile;
		ExecutionParametersManager.setConfigFile(this.configFile);
	}

	/**
	 * Gets the master conf.
	 *
	 * @return the master conf
	 */
	public String getMasterConf() {
		return this.masterConf;
	}

	/**
	 * Sets the master conf.
	 *
	 * @param masterConf the new master conf
	 */
	public void setMasterConf(String masterConf) {
		LOGGER.info("Setting Spark master configuration as " + masterConf + ".\n");

		this.masterConf = masterConf;
	}

	/**
	 * Gets the spark log level.
	 *
	 * @return the spark log level
	 */
	public Level getLogLevel() {
		return this.logLevel;
	}

	/**
	 * Gets the sequences.
	 *
	 * @return the sequences
	 */
	public JavaRDD<Sequence> getSequences() {
		return this.sequences;
	}

	/**
	 * Sets the sequences.
	 *
	 * @param sequences the new sequences
	 */
	public void setSequences(JavaRDD<Sequence> sequences) {
		this.sequences = sequences;
	}

}