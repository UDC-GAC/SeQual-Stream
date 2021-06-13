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
package com.roi.galegot.sequal.sequalmodel.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The Class ExecutionParametersManager.
 */
public final class ExecutionParametersManager {

	private static final String DEFAULT_CONFIGURATION_FILE = "/ExecutionParameters.properties";
	private static String CONFIGURATION_FILE = DEFAULT_CONFIGURATION_FILE;
	private static Map<String, String> parameters;
	private static Boolean modified = false;

	/**
	 * Instantiates a new execution parameters manager.
	 */
	private ExecutionParametersManager() {
	}

	/**
	 * Reads all the parameters from the specified CONFIGURATION_FILE and stores
	 * them in a HashMap.
	 *
	 * @return Map<String, String> pair of Name - Value of the parameters
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static synchronized Map<String, String> getParameters() {

		if (parameters == null) {
			Class<ExecutionParametersManager> configurationParametersManagerClass = ExecutionParametersManager.class;
			ClassLoader classLoader = configurationParametersManagerClass.getClassLoader();
			InputStream inputStream;

			if (modified) {
				try {
					inputStream = new FileInputStream(CONFIGURATION_FILE);
				} catch (FileNotFoundException e1) {
					throw new RuntimeException(e1);
				}
			} else {
				inputStream = classLoader.getResourceAsStream(CONFIGURATION_FILE);
			}
			if (inputStream != null) {
				Properties properties = new Properties();
				try {
					properties.load(inputStream);
					inputStream.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				parameters = new HashMap(properties);
			} else {
				parameters = new HashMap<String, String>();
			}

		}
		return parameters;

	}

	/**
	 * Returns the value of the specified parameter.
	 *
	 * @param param specified parameter
	 * @return String value of the parameter
	 */
	public static String getParameter(String param) {
		return getParameters().get(param);
	}

	/**
	 * Sets a value for the specified parameter.
	 *
	 * @param param specified parameter
	 * @param value specified value for the parameter
	 */
	public static void setParameter(String param, String value) {
		getParameters().put(param, value);
	}

	/**
	 * Sets the file.
	 *
	 * @param path the new file
	 */
	private static synchronized void setFile(String path) {
		CONFIGURATION_FILE = path;
		modified = true;
	}

	/**
	 * Sets the config file.
	 *
	 * @param path the new config file
	 */
	public static void setConfigFile(String path) {
		if (new File(path).exists()) {
			setFile(path);
		} else {
			throw new RuntimeException("Invalid config file");
		}
	}

}