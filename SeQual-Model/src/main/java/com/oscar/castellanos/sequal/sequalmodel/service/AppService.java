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
package com.oscar.castellanos.sequal.sequalmodel.service;

import java.io.IOException;
import org.apache.log4j.Level;

public interface AppService {
		
	abstract public void start();
	abstract public void stop();
	abstract public void read() throws IOException;
	abstract public void write() throws IOException;
	abstract public void writeWithSingleFile() throws IOException;
	abstract public void filter();
	abstract public void format();
	abstract public void trim();
	abstract public void measure(Boolean isFirst);
	abstract public void printStats();
	abstract public void generateConfigFile() throws IOException;
	abstract public String getParameter(String param);
	abstract public void setParameter(String param, String value);
	abstract public String getInput();
	abstract public void setInput(String input);
	abstract public String getSecondInput();
	abstract public void setSecondInput(String secondInput);
	abstract public String getOutput();
	abstract public void setOutput(String output);
	abstract public String getConfigFile();
	abstract public void setConfigFile(String configFile);
	abstract public String getMasterConf();
	abstract public void setMasterConf(String masterConf);
	abstract public Level getLogLevel();
	abstract public void setLogLevel(Level logLevel);

}
