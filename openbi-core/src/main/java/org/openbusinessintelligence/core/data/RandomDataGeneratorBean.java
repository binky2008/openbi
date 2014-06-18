package org.openbusinessintelligence.core.data;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.io.*;

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
    private int numberOfRows = 0;
    private boolean preserveDataOption = false;
    private int commitFrequency;
    
    // Internally used variables
    // Column dictionary
    private String[] columnNames = null;
    private String[] columnTypes = null;
    private String[] columnTypeAttribute = null;
    private int[] columnLengths = null;
    private int[] columnPrecisions = null;
    private int[] columnScales = null;
    
    // Data generation properties
    private String[] columnDataGenerationMethod = null;
    private String[] columnDataGenerationSource = null;
    private String[] columnDataGenerationSourceName = null;
    
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
    
    public void setNumberOfRows(int property) {
    	numberOfRows = property;
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
    	columnTypeAttribute = tableDictionary.getColumnTypeAttribute();
    	columnLengths = tableDictionary.getColumnLength();
    	columnPrecisions = tableDictionary.getColumnPrecision();
    	columnScales = tableDictionary.getColumnScale();
    	//
       	connection.getConnection().setAutoCommit(false);
        PreparedStatement targetStmt;
        logger.info("Preserve target data = " + preserveDataOption);
        String schemaPrefix = "";
        
        // Empty target table if required
        if (!preserveDataOption && !connection.getDatabaseProductName().toUpperCase().contains("IMPALA")) {
            logger.info("Truncate table");
            String truncateText = "";
            if (!(targetSchema == null || targetSchema.equals(""))) {
            	schemaPrefix = targetSchema + ".";
            }
            if (connection.getDatabaseProductName().toUpperCase().contains("TERADATA")) {
            	truncateText = "DELETE " + schemaPrefix + targetTable + " ALL";
            }
            else if (connection.getDatabaseProductName().toUpperCase().contains("FIREBIRD")) {
            	truncateText = "DELETE FROM " + schemaPrefix + targetTable;
            }
            else {
	            truncateText = "TRUNCATE TABLE " + schemaPrefix + targetTable;
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
            if (!connection.getDatabaseProductName().toUpperCase().contains("HIVE")) {
                connection.getConnection().commit();
            }
            logger.info("Table truncated");
        }
        
		int position;
        
        // Build insert string
	    logger.debug("Building insert string...");
	    
        String insertText = "INSERT ";
        if (connection.getDatabaseProductName().toUpperCase().contains("ORACLE")) {
        	insertText += "/*+APPEND*/ ";
        }
        
        insertText += "INTO ";

        if (connection.getDatabaseProductName().toUpperCase().contains("HIVE")) {
        	insertText += "TABLE ";
        }
        insertText += schemaPrefix + targetTable + " (";
        
		position = 0;
        for (int i = 0; i < columnNames.length; i++) {
        	if (getColumnUsable (columnTypes[i])) {
            	if (position > 0) {
            		insertText += ",";
            	}
            	insertText += connection.getColumnIdentifier(columnNames[i]);
    			position++;
	    	}
        }
        
	    insertText += ") VALUES (";
	    
		position = 0;
	    for (int i = 0; i < columnNames.length; i++) {
        	if (getColumnUsable (columnTypes[i])) {
    	    	if (position > 0) {
    	    		insertText = insertText + ",";
    	    	}
    	    	if (
    	              connection.getDatabaseProductName().toUpperCase().contains("MICROSOFT") &&
    	              columnTypes[i].toUpperCase().contains("BINARY")
    	        ) {
    			    insertText = insertText + "CONVERT(VARBINARY,?)";
    		    }
    		    else {
    			    insertText = insertText + "?";
    		    }
    			position++;
        	}
	    }
	    
	    insertText = insertText + ")";

	    logger.debug(insertText);
	    logger.debug("Insert string built");

	    int rowCount = 0;
	    int rowSinceCommit = 0;
	    logger.info("Commit every " + commitFrequency + " rows");
    	targetStmt = connection.getConnection().prepareStatement(insertText);
    	targetStmt.setFetchSize(commitFrequency);

    	String randomString = "";
    	int randomNumber = 0;
    	
    	// Build input stream for binary data
    	byte[] bytes = new byte[1];
    	bytes[0] = 0;
		InputStream binaryData = new ByteArrayInputStream(bytes);
		//
		java.sql.Date date;
		date = java.sql.Date.valueOf("2014-10-26");
		//
		java.sql.Time time;
		time = java.sql.Time.valueOf("12:00:00");
		//
		java.sql.Timestamp timestamp;
		timestamp = java.sql.Timestamp.valueOf("2014-10-26 12:00:00");
    	
    	// Loop for each row
    	for (int r = 0; r < numberOfRows; r++) {
	    	try {

	        	// Loop for each column
	    		position = 0;
	    		for (int i = 0; i < columnNames.length; i++) {

	            	if (getColumnUsable (columnTypes[i])) {
		    			position++;
		    			try {
		              		if (columnTypes[i].toUpperCase().contains("BOOLEAN")) {
	              				targetStmt.setBoolean(position, false);
	              			}
			    			else if (
			    				columnTypes[i].toUpperCase().contains("DATETIME") ||
			    				columnTypes[i].toUpperCase().contains("TIMESTAMP")
			    			) {
						    	targetStmt.setTimestamp(position, timestamp);
						    }
			    			else if (columnTypes[i].toUpperCase().contains("DATE")) {
						    	targetStmt.setDate(position, date);
						    }
			    			else if (columnTypes[i].toUpperCase().contains("TIME")) {
						    	targetStmt.setTime(position, time);
						    }
		              		else if (
			    				(
			    					columnTypes[i].toUpperCase().contains("CLOB") ||
			    					columnTypes[i].toUpperCase().contains("CHAR") ||
			    					columnTypes[i].toUpperCase().contains("TEXT") ||
			    					columnTypes[i].toUpperCase().contains("STRING")
			    				) &&
			    				!columnTypeAttribute[i].toUpperCase().contains("BIT")
			    			) {
					    		targetStmt.setString(position, "a");
			    			}
			    			else if (
			    				columnTypes[i].toUpperCase().contains("NUMBER") ||
			    				columnTypes[i].toUpperCase().contains("NUMERIC") ||
			    				columnTypes[i].toUpperCase().contains("SERIAL") ||
			    				columnTypes[i].toUpperCase().contains("DOU") ||
			    				columnTypes[i].toUpperCase().contains("DEC") ||
			    				columnTypes[i].toUpperCase().contains("INT") ||
			    				columnTypes[i].toUpperCase().contains("REAL") ||
			    				columnTypes[i].toUpperCase().contains("FLO") ||
			    				columnTypes[i].toUpperCase().contains("MONEY")
					    	) {
					    		targetStmt.setInt(position, 0);
					    	}
			    			else if (
				    			(
				    				columnTypes[i].toUpperCase().contains("BIN") ||
				    				columnTypes[i].toUpperCase().contains("BLOB") ||
				    				columnTypes[i].toUpperCase().contains("IMAGE")
				    			) &&
				    			!(
				              		connection.getDatabaseProductName().toUpperCase().contains("FIREBIRD") ||
				              		connection.getDatabaseProductName().toUpperCase().contains("HSQL") ||
				              		connection.getDatabaseProductName().toUpperCase().contains("INFORMIX") ||
				              		connection.getDatabaseProductName().toUpperCase().contains("TERADATA")
				    			)
						    ) {
			    				logger.error(columnNames[i] + ": " + columnTypes[i] + " - IT'S A BINARY");
						    	targetStmt.setBinaryStream(position, binaryData);
						    }
			    			else {
			    				if (
			              			connection.getDatabaseProductName().toUpperCase().contains("MICROSOFT") ||
			              			connection.getDatabaseProductName().toUpperCase().contains("ANYWHERE") ||
			              			connection.getDatabaseProductName().toUpperCase().contains("DERBY")
			              		) {
				              		if (columnTypeAttribute[i].toUpperCase().contains("BIT")) {
			              				targetStmt.setNull(position, Types.BINARY);
			              			}
				              		else if (columnTypes[i].toUpperCase().equals("UNIQUEIDENTIFIER")) {
			              				targetStmt.setNull(position, Types.BINARY);
			              			}
				              		else if (columnTypes[i].toUpperCase().contains("BLOB")) {
			              				targetStmt.setNull(position, Types.BLOB);
			              			}
			              			else if (columnTypes[i].toUpperCase().contains("FLOAT")) {
			              				targetStmt.setNull(position, Types.FLOAT);
			              			}
			              			else if (columnTypes[i].toUpperCase().contains("REAL")) {
			              				targetStmt.setNull(position, Types.REAL);
			              			}
			              			else if (columnTypes[i].toUpperCase().contains("TEXT")) {
			              				targetStmt.setNull(position, Types.CHAR);
			              			}
				              		else if (columnTypes[i].toUpperCase().contains("TIMESTAMP")) {
			              				targetStmt.setNull(position, Types.TIMESTAMP);
			              			}
			              			else if (columnTypes[i].toUpperCase().contains("DATE")) {
			              				targetStmt.setNull(position, Types.DATE);
			              			}
			              			else if (columnTypes[i].toUpperCase().contains("TIME")) {
			              				targetStmt.setNull(position, Types.TIME);
			              			}
					              	else {
				    					targetStmt.setNull(position, Types.NULL);
					              	}
			              		}
				              	else {
			    					targetStmt.setNull(position, Types.NULL);
				              	}
			    			}
		    			}
		    			catch (Exception e){
		    				logger.error(columnNames[i] + ": " + columnTypes[i] + " - Error: " + e.getMessage());
		    				throw e;
		    			}
	            	}
	    		}
		    	targetStmt.executeUpdate();
		    	targetStmt.clearParameters();
	        }
	        catch(Exception e) {
	        	logger.error("Unexpected exception, list of columns:");
	        	for (int i = 0; i < columnNames.length; i++) {
	        		try {
	        			logger.error(columnNames[i] + ": " + columnTypes[i]);
				    }
	        		catch(NullPointerException npe) {
	        			logger.error(columnNames[i]);
			        }
	            }
	            logger.error(e.getMessage());
	            throw e;
	        }
		    	
	    	rowCount++;
	    	rowSinceCommit++;
	    	if (rowSinceCommit==commitFrequency) {
	            if (!connection.getDatabaseProductName().toUpperCase().contains("IMPALA")) {
	                connection.getConnection().commit();
	            }
	    		rowSinceCommit = 0;
	    		logger.info(rowCount + " rows inserted");
	    	}
    	}
    	targetStmt.close();
        if (!connection.getDatabaseProductName().toUpperCase().contains("IMPALA")) {
            connection.getConnection().commit();
        }

	    logger.info(rowCount + " rows totally inserted");
	    logger.info("GENERATION COMPLETED");
	    logger.info("########################################");
    }
    
    private boolean getColumnUsable (String dataType) {
       	if (
           	(dataType.toUpperCase().contains("MDSYS.SDO")) ||
       		(dataType.toUpperCase().contains("INTERVAL")) ||
       		(dataType.toUpperCase().contains("SERIAL")) ||
       		(dataType.toUpperCase().contains("POINT")) ||
       		(dataType.toUpperCase().contains("FILE")) ||
       		(
       			connection.getDatabaseProductName().toUpperCase().contains("MICROSOFT") &&
       			dataType.toUpperCase().contains("TIMESTAMP")
       		)
        ) {
        	return false;
       	}
       	else {
        	return true;
       	}
    }
}
