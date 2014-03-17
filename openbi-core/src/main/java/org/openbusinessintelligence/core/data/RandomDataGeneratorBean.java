package org.openbusinessintelligence.core.data;

import java.sql.PreparedStatement;
import java.sql.Types;

import org.openbusinessintelligence.core.db.*;
import org.slf4j.LoggerFactory;

public class RandomDataGeneratorBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(RandomDataGeneratorBean.class);

    // Declarations of bean properties
	// Source properties
    private ConnectionBean connection = null;
    private TableDictionaryBean tableDictionary = null;
    private String targetSchema = "";
    private String targetTable = "";
    private int rowCount = 0;
    private boolean preserveDataOption = false;
    private int commitFrequency;
    
    // Internally used variables
    private String[] columnNames = null;
    private String[] columnTypes = null;
    private int[] columnLengths = null;
    private int[] columnPrecisions = null;
    private int[] columnScales = null;
    
    // Constructor
    public RandomDataGeneratorBean() {
        super();
        tableDictionary = new TableDictionaryBean();

    }

    // Set properties methods    
    public void setConnection(ConnectionBean property) {
    	connection = property;
    	tableDictionary.setSourceConnection(connection);
    }
    
    public void setTargetSchema(String property) {
    	targetSchema = property;
    }
    
    public void setTargetTable(String property) {
    	targetTable = property;
    }

    public void setPreserveDataOption(boolean tt) {
    	preserveDataOption = tt;
    }
    
    public void setCommitFrequency(int cf) {
        commitFrequency = cf;
    }
    
    public void setRowCount(int property) {
    	rowCount = property;
    }
    
    public void generateData() throws Exception {
        logger.info("########################################");
    	logger.info("GENERATING DATA...");


    	// Get table dictionary
    	tableDictionary.setSourceTable(targetTable);
    	tableDictionary.retrieveColumns();
    	//
    	columnNames = tableDictionary.getColumnNames();
    	columnTypes = tableDictionary.getColumnTypes();
    	columnLengths = tableDictionary.getColumnLength();
    	columnPrecisions = tableDictionary.getColumnPrecision();
    	columnScales = tableDictionary.getColumnScale();
    	//
       	connection.getConnection().setAutoCommit(false);
        PreparedStatement targetStmt;
        logger.info("Preserve target data = " + preserveDataOption);
        if (!preserveDataOption) {
            logger.info("Truncate table");
            String truncateText = "";
            if (connection.getDatabaseProductName().toUpperCase().contains("TERADATA")) {
            	truncateText = "DELETE " + targetSchema + "." + targetTable + " ALL";
            }
            else {
	            truncateText = "TRUNCATE TABLE " + targetSchema + "." + targetTable;
	           	if (connection.getDatabaseProductName().toUpperCase().contains("DB2")) {
	           		connection.closeConnection();
	           		connection.openConnection();
	           		truncateText += " IMMEDIATE";
	           	}
            }
            logger.debug(truncateText);
           	targetStmt = connection.getConnection().prepareStatement(truncateText);
            targetStmt.executeUpdate();
            targetStmt.close();
            connection.getConnection().commit();
            logger.info("Table truncated");
        }
        
        String insertText = "INSERT /*+APPEND*/ INTO " + targetSchema + "." + targetTable + " (";
        for (int i = 0; i < columnNames.length; i++) {
        	if (i > 0) {
        		insertText += ",";
        	}
        	insertText += connection.getColumnIdentifier(columnNames[i]);
        }
        
	    insertText += ") VALUES (";
	    
	    for (int i = 0; i < columnNames.length; i++) {
	    	if (i > 0) {
	    		insertText = insertText + ",";
	    	}
	    	insertText = insertText + "?";
	    }
	    
	    insertText = insertText + ")";
	    
	    logger.debug(insertText);
	    logger.debug("Statement prepared");
	    
	    int rowCount = 0;
	    int rowSinceCommit = 0;
	    logger.info("Commit every " + commitFrequency + " rows");
    	targetStmt = connection.getConnection().prepareStatement(insertText);
    	targetStmt.setFetchSize(commitFrequency);
    	for (int r = 0; r < rowCount; r++) {
	    	try {
	    		int position = 0;
	    		
	    		for (int i = 0; i < columnNames.length; i++) {
	    			position++;
	    			try {
			    		targetStmt.setObject(position, 0);
	    			}
	    			catch (Exception e){
	              		if (connection.getDatabaseProductName().toUpperCase().contains("TERADATA")) {
	              			targetStmt.setNull(position, Types.NULL);
		              	}
		              	else {
		              		targetStmt.setObject(position, null);
		              	}
	    			}
	    		}
		    	targetStmt.executeUpdate();
		    	targetStmt.clearParameters();
	        }
	        catch(Exception e) {
	        	logger.error("Unexpected exception, list of column values:");
	        	for (int i = 0; i < columnNames.length; i++) {
	        		try {
	        			logger.error("########################################\n" + columnNames[i] + " ==> ");
				    }
	        		catch(NullPointerException npe) {
	        			logger.error("########################################\n" + columnNames[i]);
			        }
	            }
	            logger.error(e.getMessage());
	            throw e;
	        }
		    	
	    	rowCount++;
	    	rowSinceCommit++;
	    	if (rowSinceCommit==commitFrequency) {
	    		connection.getConnection().commit();
	    		rowSinceCommit = 0;
	    		logger.info(rowCount + " rows inserted");
	    	}
    	}
    	targetStmt.close();
        connection.getConnection().commit();

	    logger.info(rowCount + " rows totally inserted");
	    logger.info("GENERATION COMPLETED");
	    logger.info("########################################");
    }
}
