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

import org.apache.log4j.Logger;

import com.oscar.castellanos.sequal.sequalmodel.service.AppService;
import com.roi.galegot.sequal.sequalmodel.service.AppServiceBatch;
import com.roi.galegot.sequal.sequalmodel.util.ExecutionParametersManager;

public class AppServiceFactory {
	
	private static final Logger LOGGER = Logger.getLogger(AppServiceFactory.class.getName());
	private final static String MODE_PARAMETER = "Mode";
	private final static String STREAM_MODE = "STREAMING";
	private final static String BATCH_MODE = "BATCH";
	private static AppService appService = null;
	
	private AppServiceFactory() {
	}

	/**
	 * Returns an AppServiceInterface based on the configuration file.
	 *
	 * @param configFile String the configuration file
	 * @return AppServiceInterface
	 */
	private static AppService getInstance(String configFile) {
		ExecutionParametersManager.setConfigFile(configFile);
		String mode = ExecutionParametersManager.getParameter(MODE_PARAMETER);
		
		if (mode.compareToIgnoreCase(STREAM_MODE) == 0) {
			return new AppServiceStream();
		}
		if (mode.compareToIgnoreCase(BATCH_MODE) == 0) {
			return new AppServiceBatch();
		}
		LOGGER.warn("Mode (Streaming or Batch) not specified. Using Batch mode");
		return new AppServiceBatch();
	}
	
	public static synchronized AppService getAppService(String configFile) {
		if (appService == null) {
			appService = getInstance(configFile);
		}
		return appService;
	}
	
	/**
	 * Returns an AppServiceInterface based on a boolean.
	 *
	 * @param streaming if the mode is streaming or not
	 * @return AppServiceInterface
	 */
	public static synchronized AppService getAppService(boolean streaming) {
		if (streaming) {
			return new AppServiceStream();
		}
		return new AppServiceBatch();
	}
	
	// used when configFile is not specified and we want to create a template
	public static synchronized AppService getAppService() {
		return new AppServiceBatch();
	}
}
