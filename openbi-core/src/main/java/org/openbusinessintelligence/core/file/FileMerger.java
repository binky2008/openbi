package org.openbusinessintelligence.core.file;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import org.slf4j.LoggerFactory;

public class FileMerger {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(FileMerger.class);

    // Declarations of bean properties
	private String inputZipFile = "";
	private String inputDirectory = "";
	private String[] outputFileNames = null;
	private String[] distributionPatterns = null;
	private boolean addFileNameOption = false;
	private String columnSeparator = "";

    // Constructor
	public FileMerger() {
		super();
	}
	
    // Set properties methods
	public void setInputZipFile(String property) {
		inputZipFile = property;
	}
	
	public void setInputDirectory(String property) {
		inputDirectory = property;
	}
	
	public void setOutputFileNames(String[] property) {
		outputFileNames = property;
	}
	
	public void setDistributionPatterns(String[] property) {
		distributionPatterns = property;
	}
	
	public void setAddFileNameOption(boolean property) {
		addFileNameOption = property;
	}
	
	public void setColumnSeparator(String property) {
		columnSeparator = property;
	}
	
    // Execution methods
	public void mergeFiles() throws Exception {

		ArrayList<BufferedWriter> outputFiles = new ArrayList<BufferedWriter>();
		for (int i=0; i<outputFileNames.length; i++) {
			FileExporter outputFile = new FileExporter();
			outputFile.setFileName(outputFileNames[i]);
			outputFiles.add(new BufferedWriter(outputFile.getWriter()));
		}
		
    	if (!(inputZipFile == null || inputZipFile.equals("")) ) {
    		
        	logger.info("Reading entries in zip file " + inputZipFile);
    		
    		ZipFile zipFile = new ZipFile(inputZipFile);
    		Enumeration<? extends ZipEntry> entries = zipFile.entries();
    		
    		while(entries.hasMoreElements()) {
    			ZipEntry entry = (ZipEntry)entries.nextElement();
    			if(!entry.isDirectory()) {
    				logger.info("IMPORTING ENTRY " + entry.getName());
    				BufferedReader reader = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry)));
    				
    				String line;
    				while ((line=reader.readLine()) != null) {
	    				for (int i=0; i<outputFileNames.length; i++) {
	    					if (line.startsWith(distributionPatterns[i]) ) {
	    						if (addFileNameOption) {
	    							line = entry.getName() + columnSeparator + line;
	    						}
	    						outputFiles.get(i).write(line);
	    						outputFiles.get(i).newLine();
	    					}
	    				}
    				}
    				
            		reader.close();
    				logger.info("ENTRY " + entry.getName() + " IMPORTED");
    			}
    		}
    		zipFile.close();
    		
        	logger.info("ZIP FILE " + inputZipFile + " COMPLETED");
    	}
		for (int i=0; i<outputFileNames.length; i++) {
			outputFiles.get(i).close();
		}

	}
}
