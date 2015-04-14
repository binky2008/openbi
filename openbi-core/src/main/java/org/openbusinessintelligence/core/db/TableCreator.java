package org.openbusinessintelligence.core.db;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.slf4j.LoggerFactory;

public class TableCreator {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TableCreator.class);

    // Target properties
    private DBConnection targetCon = null;
    private String targetSchema = "";
    private String targetTable = "";
    private String[] targetColumns = null;
    private String[] targetColumnDefinitions = null;
    // Options
    private boolean dropIfExists = false;
    
    // Internally used
    private int position = 0;
    
    // Constructor
    public TableCreator() {
        super();
    }

    public void setTargetSchema(String property) {
        targetSchema = property;
    }

    public void setTargetTable(String property) {
        targetTable = property;
    }

    public void setTargetColumns(String[] property) {
    	targetColumns = property;
    }
    
    public void setTargetColumnDefinitions(String[] property) {
    	targetColumnDefinitions = property;
    }
    
    public void setTargetConnection(DBConnection property) {
    	targetCon = property;
    }
    
    public void setDropIfExistsOption(boolean property) {
    	dropIfExists = property;
    }

    
    // Execution methods    
    public void createTable() throws Exception {
    	logger.info("########################################");
    	logger.info("CREATING TABLE");
    	
    	String sqlText;
    	String searchSchema;
    	
    	if (
    		!(targetSchema == null || targetSchema.equals("")) &&
    		!targetCon.getDatabaseProductName().toUpperCase().contains("POSTGRES")
    	) {
    		searchSchema = targetSchema.toUpperCase();
    	}
    	else {
    		searchSchema = targetSchema;
    	}
    	
    	boolean tableExistsFlag = false;
    	
    	DatabaseMetaData dbmd = targetCon.getConnection().getMetaData();
    	ResultSet tables;

		logger.debug("Schema: " + searchSchema);
		if (targetCon.getDatabaseProductName().toUpperCase().contains("IMPALA")) {
	   		tables = dbmd.getTables(null, searchSchema.toLowerCase(), null, null);
		}
		else {
	   		tables = dbmd.getTables(null, searchSchema, null, null);
		}
   		
    	while(tables.next()) {
    		logger.debug("Found table: " + tables.getString(2) + "." + tables.getString(3));
    		if (tables.getString(3).toUpperCase().equals(targetTable.toUpperCase())) {
    			tableExistsFlag = true;
        		logger.debug("Table exists");
    		}
    	}
    	tables.close();
        logger.info("Drop table if it exists: " + String.valueOf(dropIfExists));
        logger.info("Table exists:            " + String.valueOf(tableExistsFlag));
    	
        try {
	    	if ((dropIfExists == true ) && (tableExistsFlag == true)) {
	            logger.info("Drop table");
	            
		    	if (!(targetSchema == null || targetSchema.equals(""))) {
			       	sqlText = "DROP TABLE " + targetSchema + "." + targetTable;
		    	}
		    	else {
			       	sqlText = "DROP TABLE " + targetTable;
		    	}
	    	
	    		// Drop existing table
		        logger.debug("Drop statement:\n" + sqlText);
		
		       	// Execute prepared statement
		        PreparedStatement targetStmt;
		    	targetStmt = targetCon.getConnection().prepareStatement(sqlText);
		    	targetStmt.executeUpdate();
		    	targetStmt.close();
	            logger.info("Table dropped");
	    		
	    	}
	    	
	    	position = 0;
	    	if ((tableExistsFlag == false) || (dropIfExists == true )) {
		    	
		        logger.info("Create table");
	    	
	    		// create table
		        sqlText = "CREATE ";
		        if (targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")) {
			        sqlText += "MULTISET ";
		        }
		        else if (targetCon.getDatabaseProductName().toUpperCase().contains("HDB")) {
			        sqlText += "COLUMN ";
		        }
		    	if (!(targetSchema == null || targetSchema.equals(""))) {
			       	sqlText += "TABLE " + targetSchema + "." + targetTable + "(";
		    	}
		    	else {
			       	sqlText += "TABLE " + targetTable + "(";
		    	}
		       	for (int i = 0; i < targetColumnDefinitions.length; i++) {
		       		
		       		if (targetColumnDefinitions[i].equals("")) {
		       			logger.debug("Column: " + targetColumns[i] + " type non supported");
		       		}
		       		else {
				    	if (position > 0) {
				    		sqlText += ",";
				    	}
			       		sqlText += targetColumns[i] + " " + targetColumnDefinitions[i];
				        if (targetCon.getDatabaseProductName().toUpperCase().contains("ANYWHERE")) {
					        sqlText += " NULL";
				        }
				        position++;
		       		}
		       	}
		       	sqlText += ")";
		
		       	// Execute prepared statement
		        PreparedStatement targetStmt;
		        logger.debug("Creation statement:\n" + sqlText);
		    	targetStmt = targetCon.getConnection().prepareStatement(sqlText);
		    	targetStmt.executeUpdate();
		    	targetStmt.close();
		    	
		        logger.info("Table created");
	    	}
        }
        catch (Exception e) {
        	logger.error(e.toString());
        	throw e;
        }
    	
        logger.info("########################################");
    	
    }
    
}