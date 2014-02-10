package org.openbusinessintelligence.core.db;

import java.util.*;
import java.util.zip.*;

import org.slf4j.LoggerFactory;

public class ImportCsvSeriesBean {
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ImportCsvSeriesBean.class);

    // Declarations of bean properties
	// Source properties
    private ConnectionBean sourceCon = null;
    private String sourceZipFile = "";
    private String sourceWhereClause = "";

    // Target properties
    private ConnectionBean targetCon = null;
    private String targetTable = "";
	private String fileNameColumn = "";
    
    // Execution properties
    private int commitFrequency;
	
	/**
	 * Constructor
	 */
	public ImportCsvSeriesBean() {
		super();
	}

    // Set source properties methods
    public void setSourceConnection(ConnectionBean property) {
    	sourceCon = property;
    }
    
    public void setSourceZipFile(String szf) {
    	sourceZipFile = szf;
    }

    public void setSourceWhereClause(String swc) {
        sourceWhereClause = swc;
    }

    // Set target properties methods
    public void setTargetConnection(ConnectionBean property) {
    	targetCon = property;
    }
    
    public void setTargetTable(String tt) {
        targetTable = tt;
    }

    public void setFileNameColumn(String fnc) {
    	fileNameColumn = fnc;
    }
    // Set optional execution properties 
    public void setCommitFrequency(int cf) {
        commitFrequency = cf;
    }
    
    // Import a series of zip files
    public void importCsvSeries() throws Exception {
    	
		org.openbusinessintelligence.core.db.DataCopyBean dataCopy = new org.openbusinessintelligence.core.db.DataCopyBean();

		dataCopy.setSourceConnection(sourceCon);

		dataCopy.setTargetConnection(targetCon);
		dataCopy.setTargetTable(targetTable);		
		dataCopy.setPreserveDataOption(true);
		
		String[] fileNameCol = new String[1];
		String[] fileNameValue = new String[1];
		
		if (!(fileNameColumn == null || fileNameColumn.equals("")) ) {
			fileNameCol[0] = fileNameColumn;
		}
		
		dataCopy.setCommitFrequency(commitFrequency);
    		
       	logger.info("LOADING ENTRIES IN ZIP FILE " + sourceZipFile);
   		
   		// Loop on zip entries
   		ZipFile zipFile = new ZipFile(sourceZipFile);    		
   		Enumeration<? extends ZipEntry> entries = zipFile.entries();
   		
   		int i = 0;
   		while(entries.hasMoreElements()) {
   			ZipEntry entry = (ZipEntry)entries.nextElement();
   			if(!entry.isDirectory()) {
   				
   				logger.info("IMPORTING ENTRY " + entry.getName());
   				
   				dataCopy.setSourceQuery("SELECT * FROM " + entry.getName() + " WHERE " + sourceWhereClause);
           		
           		if (i == 0) {
           			if (!(fileNameColumn == null || fileNameColumn.equals("")) ) {
           				dataCopy.setTargetDefaultColumns(fileNameCol);
           			}
           			dataCopy.retrieveColumnList();
           		}
            		if (!(fileNameColumn == null || fileNameColumn.equals("")) ) {
            		fileNameValue[0] = entry.getName();
            		dataCopy.setTargetDefaultValues(fileNameValue);
            	}
           		
           		dataCopy.executeSelect();
           		dataCopy.executeInsert();
       			i += 1;
   				logger.info("ENTRY " + entry.getName() + " IMPORTED");
   			}
   		}
   		zipFile.close();
   		
       	logger.info("ZIP FILE " + sourceZipFile + " COMPLETED");
   	}
}