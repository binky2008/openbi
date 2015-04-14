package org.openbusinessintelligence.core.file;

import java.io.*;
import java.sql.*;
import javax.sql.rowset.*;

import org.slf4j.LoggerFactory;

/**
 * Utility class to facilitate the use of files as output targets
 * @author marangon
 */
public class FileExporter {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(FileExporter.class);

	private WebRowSet webRS;
	private String contentString;
	private String directoryName = "";
	private String fileName;
	private Writer writer;

	public FileExporter() {
		super();
	}

	// Set property methods
	public void setDirectoryName(String property) {
		directoryName = property;
	}

	public void setFileName(String property) {
		fileName = property;
	}

	public void setWebRS(WebRowSet property) {
		webRS = property;
	}

	public void setContentString(String property) {
		contentString = property;
	}

	// Instantiate the writer object
	public Writer getWriter() throws Exception {
		File outputFile = null;
		if (directoryName.equals("")) {
			outputFile = new File(fileName);
		}
		else {
			outputFile = new File(directoryName + File.separatorChar + fileName);
		}
		try {
			writer = new java.io.FileWriter(outputFile);
		}
		catch (IOException e) {
			System.out.println("Cannot create output file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
		return writer;
	}

	// Write content
	public void writeContentString() throws Exception {
		File outputFile = null;
		if (directoryName.equals("")) {
			outputFile = new File(fileName);
		}
		else {
			outputFile = new File(directoryName + File.separatorChar + fileName);
		}
		try {
			writer = new java.io.FileWriter(outputFile);
			BufferedWriter buffWriter = new BufferedWriter(writer);
			buffWriter.write(contentString);
			buffWriter.close();
		}
		catch (IOException e) {
			System.out.println("Cannot create output file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
	}

	public void writeWRS() throws Exception {
		File outputFile = null;
		if (directoryName.equals("")) {
			outputFile = new File(fileName);
		}
		else {
			outputFile = new File(directoryName + File.separatorChar + fileName);
		}
		try {
			outputFile = new File(directoryName + File.separatorChar + fileName);
			writer = new java.io.FileWriter(outputFile);
		}
		catch (IOException e) {
			System.out.println("Cannot create output file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
		try {
			webRS.writeXml(writer);
		}
		catch (SQLException e) {
			System.out.println("Cannot write output to file" + directoryName + File.separatorChar + fileName);
			System.out.println(e);
			throw e;
		}
	}
}
