package org.openbusinessintelligence.core.db;

import java.io.FileInputStream;
import java.sql.*;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.sql.DataSource;
import org.slf4j.LoggerFactory;

public class TableCreateBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TableCreateBean.class);

    // Target properties
    private ConnectionBean targetCon = null;
    private String targetCatalog = "";
    private String targetSchema = "";
    private String targetTable = "";
    private String[] targetColumns = null;
    private String[] targetColumnDefinitions = null;
    
    // Options
    private boolean dropIfExists = false;
    
    // Constructor
    public TableCreateBean() {
        super();
    }
    
    // Set target properties methods
    public void setTargetCatalog(String property) {
        targetCatalog = property;
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
    
    public void setTargetConnection(ConnectionBean property) {
    	targetCon = property;
    }
    
    public void setDropIfExistsOption(boolean property) {
    	dropIfExists = property;
    }

    
    // Execution methods    
    public void createTable() throws Exception {
    	logger.info("########################################");
    	logger.info("CREATING TABLE");
    	
    	String currentCatalog;
    	String currentSchema;
    	String sqlText;
    	
    	boolean tableExistsFlag = false;
    	
    	DatabaseMetaData dbmd = targetCon.getConnection().getMetaData();
    	ResultSet tables;
    	if (
    		targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE") ||
    		targetCon.getDatabaseProductName().toUpperCase().contains("DB2") ||
    		targetCon.getDatabaseProductName().toUpperCase().contains("HDB")
    	) {
    		tables = dbmd.getTables(null, targetSchema.toUpperCase(), targetTable.toUpperCase(), null);
    	}
    	else {
    		tables = dbmd.getTables(null, targetSchema, null, null);
    	}
    	while(tables.next()) {
    		logger.debug("Found table: " + tables.getString(3));
    		if (tables.getString(3).toUpperCase().equals(targetTable.toUpperCase())) {
    			tableExistsFlag = true;
        		logger.debug("Table exists");
    		}
    	}
        logger.info("Drop table if it exists: " + String.valueOf(dropIfExists));
        logger.info("Table exists:            " + String.valueOf(tableExistsFlag));
    	
        try {
	    	if ((dropIfExists == true ) && (tableExistsFlag == true)) {
	            logger.info("Drop table");
	    	
	    		// Drop existing table
		       	sqlText = "DROP TABLE " + targetSchema + "." + targetTable;
		        logger.debug("Drop statement:\n" + sqlText);
		
		       	// Execute prepared statement
		        PreparedStatement targetStmt;
		    	targetStmt = targetCon.getConnection().prepareStatement(sqlText);
		    	targetStmt.executeUpdate();
		    	targetStmt.close();
	            logger.info("Table dropped");
	    		
	    	}
	    	if ((tableExistsFlag == false) || (dropIfExists == true )) {
		    	
		        logger.info("Create table");
	    	
	    		// create table    	
		       	sqlText = "CREATE TABLE " + targetSchema + "." + targetTable + "(";
		       	for (int i = 0; i < targetColumnDefinitions.length; i++) {
			    	if (i > 0) {
			    		sqlText += ",";
			    	}
		       		sqlText += targetColumns[i] + " " + targetColumnDefinitions[i];
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