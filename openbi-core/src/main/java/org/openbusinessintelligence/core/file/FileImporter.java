/*
Copyright 2013 Open Business Intelligence

This file is part of Foobar.

Open BI Tools is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

Open BI Tools is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with Open BI Tools. If not, see http://www.gnu.org/licenses/.
*/

package org.openbusinessintelligence.core.file;

import java.io.*;
import java.net.*;

import org.slf4j.LoggerFactory;

/**
 * Utility class to facilitate the use of files as input sources
 * @author marangon
 */

public class FileImporter {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(FileImporter.class);

    // Declarations of bean properties
	private URI fileURI = null;
	private String directoryName = "";
	private String fileName = "";
	private String filePath = "";
	private String fileContent = "";
	private BufferedReader reader;
	private File file = null;

    // Constructor
	public FileImporter() {
		super();
	}

    // Set properties methods
	public void setFileURI(URI property) {
		fileURI = property;
	}

	public void setDirectoryName(String property) {
		directoryName = property;
	}

	public void setFileName(String property) {
		fileName = property;
	}

	public void setFilePath(String property) {
		filePath = property;
	}

    // Execution methods
	public BufferedReader getReader() throws Exception {
		createFile();
		FileReader fileReader = null;
		try {
			fileReader = new FileReader(file);
			reader =new BufferedReader(fileReader);
		}
		catch (Exception e) {
			logger.error("File " + filePath + " not found:\n" + e.getMessage());
			throw e;
		}
		return reader;
	}

	public String getString() throws Exception {
		createFile();
		FileReader fileReader = null;
		BufferedReader bufferedReader = null;
		try {
			//logger.debug(file.getAbsolutePath());
			logger.debug(String.valueOf(file.exists()));
			fileReader = new FileReader(file);
			bufferedReader =new BufferedReader(fileReader);
		}
		catch (Exception e) {
			logger.error("File " + filePath + " not found:\n" + e.toString());
			throw e;
		}
		String lineBuffer;
		fileContent ="";
		try {
			while ((lineBuffer=bufferedReader.readLine()) != null) {
				fileContent += lineBuffer + "\n";
			}
		}
		catch (Exception e) {
			logger.error("File IO error:\n" + e.getMessage());
			bufferedReader.close();
			throw e;
		}
		bufferedReader.close();
		return fileContent;
	}
	
	private void createFile() throws Exception {
		if (!(fileURI == null)) {
			file = new File(fileURI);
		}
		else {
			if (filePath.equals("")) {
				if (directoryName.equals("")) {
					filePath = fileName;
				}
				else {
					filePath = directoryName + File.separatorChar + fileName;
				}
			}
		file = new File(filePath);
		}
		logger.debug("Absolute path: " + file.getAbsolutePath());
	}
}
