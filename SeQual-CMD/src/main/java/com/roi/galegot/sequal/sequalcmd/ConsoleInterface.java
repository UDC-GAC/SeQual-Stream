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
package com.roi.galegot.sequal.sequalcmd;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.roi.galegot.sequal.sequalmodel.service.AppService;

/**
 * The Class ConsoleInterface.
 */
public class ConsoleInterface {

	private static final Logger LOGGER = Logger.getLogger(ConsoleInterface.class.getName());

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IOException {
		run(args);
	}

	/**
	 * Run.
	 *
	 * @param args the args
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void run(String[] args) throws IOException {
		int position;
		int lengthArgs;

		String output;
		String input;
		String secondInput;
		String configFile;
		String masterConf;

		Level logLevel;

		Boolean writeStats;

		AppService service;

		lengthArgs = args.length;

		if (lengthArgs == 0) {
			instructions();
			return;
		}

		position = findOption(args, ConsoleOptions.OUTPUT.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			LOGGER.error("Output path not specified correctly.\n");
			instructions();
			return;
		}
		output = args[position + 1];

		position = findOption(args, ConsoleOptions.GENERATECONFIGFILE.getOpt());
		if (position != -1) {
			if (lengthArgs == 3) {
				service = new AppService();
				service.setOutput(output);
				service.generateConfigFile();
				return;
			} else {
				LOGGER.error("Incorrect number of parameters.\n");
				instructions();
				return;
			}
		}

		if (lengthArgs < 6) {
			LOGGER.error("Incorrect number of parameters.\n");
			instructions();
			return;
		}

		position = findOption(args, ConsoleOptions.INPUT.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			LOGGER.error("Input file not specified correctly.\n");
			instructions();
			return;
		}
		input = args[position + 1];

		position = findOption(args, ConsoleOptions.DOUBLEINPUT.getOpt());
		if (position != -1) {
			if (position == (lengthArgs - 1)) {
				LOGGER.error("Second input file not specified correctly.\n");
				instructions();
				return;
			} else {
				secondInput = args[position + 1];
			}
		} else {
			secondInput = "";
		}

		position = findOption(args, ConsoleOptions.CONFIGFILE.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			LOGGER.error("Execution parameters file not specified correctly.\n");
			instructions();
			return;
		}
		configFile = args[position + 1];

		position = findOption(args, ConsoleOptions.SPARKMASTERCONF.getOpt());
		if ((position != -1) && (position != (lengthArgs - 1))) {
			masterConf = args[position + 1];
		} else {
			masterConf = "";
		}

		position = findOption(args, ConsoleOptions.LOGCONF.getOpt());
		if ((position == -1) || (position == (lengthArgs - 1))) {
			LOGGER.warn("Spark logger level not specified. ERROR level will be used.\n");
			logLevel = Level.ERROR;
		} else {
			logLevel = Level.toLevel(args[position + 1]);
		}

		service = new AppService();

		service.setInput(input);
		service.setOutput(output);
		service.setConfigFile(configFile);
		service.setLogLevel(logLevel);
		if (StringUtils.isNotBlank(masterConf)) {
			service.setMasterConf(masterConf);
		}

		if (StringUtils.isNotBlank(secondInput)) {
			service.setSecondInput(secondInput);
		}

		service.start();
		service.read();

		if (findOption(args, ConsoleOptions.MEASURE.getOpt()) != -1) {
			writeStats = true;
			service.measure(true);
		} else {
			writeStats = false;
		}

		if (findOption(args, ConsoleOptions.TRIM.getOpt()) != -1) {
			service.trim();
		}

		if (findOption(args, ConsoleOptions.FILTER.getOpt()) != -1) {
			service.filter();
		}

		if (findOption(args, ConsoleOptions.FORMAT.getOpt()) != -1) {
			service.format();
		}

		if (writeStats) {
			service.measure(false);
		}

		if (findOption(args, ConsoleOptions.SINGLEFILEOUTPUT.getOpt()) != -1) {
			service.writeWithSingleFile();
		} else {
			service.write();
		}

		if (writeStats) {
			service.printStats();
		}

		service.stop();
	}

	/**
	 * Find option.
	 *
	 * @param args the args
	 * @param opt  the opt
	 * @return the int
	 */
	private static int findOption(String[] args, String opt) {
		return Arrays.asList(args).indexOf(opt);
	}

	/**
	 * Instructions.
	 */
	private static void instructions() {
		System.out.println("\nConfiguration options:");
		System.out.println("    -i InputFile: Specifies input file from where sequences will be read");
		System.out.println(
				"    -di InputFile: Specifies second input file from where sequences will be read, in case of paired-end sequences");
		System.out
				.println("    -o OuputDirectory: Specifies output directory where resulting sequences will be written");
		System.out.println("    -c ConfigFile: Specifies run options configuration file");
		System.out.println("    -g: Generates a blank configuration file in the specified with -o location");
		System.out.println("    -smc SparkMasterConf: Specifies Spark master configuration (local[*] by default)");
		System.out.println("    -lc SparkLoggerConf: Specifies Spark log configuration (ERROR by default)");

		System.out.println("\nAvailable options:");
		System.out.println("    -f: Filters read sequences following specified parameters");
		System.out.println("    -fo: Formats read sequences following specified parameters");
		System.out.println("    -t: Trims read sequences following specified parameters");
		System.out.println(
				"    -s: Calculates statistics before and after performing transformations on the read sequences");
		System.out.println(
				"    -sfo: Generates a single file containing result sequences inside the output directory named {input-file-name}-results.{format}, along with a folder named Parts containing HDFS files");

		System.out.println("\n");
	}

}