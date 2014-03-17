package org.openbusinessintelligence.core.db;

import java.util.*;
import java.io.*;
import java.sql.*;

import javax.naming.*;
import javax.sql.*;
import javax.xml.parsers.*;

import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

/**
 * Class for replication of database tables between databases
 * @author Nicola Marangoni
 */
public class DataCopyBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataCopyBean.class);

    // Declarations of bean properties
	// Source properties
    private ConnectionBean sourceCon = null;
    private String sourceTable = "";
    private String sourceQuery = "";
    private String[] queryParameters = null;
    private String[] sourceColumnNames = null;
    private String[] sourceColumnType = null;

    // Target properties
    private ConnectionBean targetCon = null;
    private String targetSchema = "";
    private String targetTable = "";
    private boolean preserveDataOption = false;
    private String[] targetColumnNames = null;
    private String[] targetColumnType = null;
    
    // Mapping properties
    private String mappingDefFile = "";
    private String[] sourceMapColumns = null;
    private String[] targetMapColumns = null;
    private String[] targetDefaultColumns = null;
    private String[] targetDefaultValues = null;
    
    // Execution properties
    private int commitFrequency;

    // Declarations of internally used variables
	TableDictionaryBean sourceDictionaryBean;
	TableDictionaryBean targetDictionaryBean;
    private String[] commonColumnNames = null;
    private String[] commonColumnTypes = null;
    private ResultSet sourceRS = null;
    private PreparedStatement sourceStmt= null;
    
    // Constructor
    public DataCopyBean() {
        super();
        sourceDictionaryBean = new TableDictionaryBean();
        targetDictionaryBean = new TableDictionaryBean();
    }

    // Set source properties methods
    public void setSourceConnection(ConnectionBean property) {
    	sourceCon = property;
    	sourceDictionaryBean.setSourceConnection(sourceCon);
    }

    public void setSourceTable(String ta) {
        sourceTable = ta;
    }

    public void setSourceQuery(String sq) {
        sourceQuery = sq;
    }

    public void setQueryParameters(String[] qp) {
        queryParameters = qp;
    }

    // Set target properties methods
    public void setTargetConnection(ConnectionBean property) {
    	targetCon = property;
		targetDictionaryBean.setSourceConnection(targetCon);
    }
    
    public void setTargetSchema(String property) {
        targetSchema = property;
    }
    
    public void setTargetTable(String ta) {
        targetTable = ta;
    }

    public void setPreserveDataOption(boolean tt) {
    	preserveDataOption = tt;
    }

    // Set optional mapping properties 
    public void setMappingDefFile(String mdf) {
    	mappingDefFile = mdf;
    }
    
    public void setSourceMapColumns(String[] smc) {
    	sourceMapColumns = smc;
    }
    
    public void setTargetMapColumns(String[] tmc) {
    	targetMapColumns = tmc;
    }
    
    public void setTargetDefaultColumns(String[] tdc) {
    	targetDefaultColumns = tdc;
    }
    
    public void setTargetDefaultValues(String[] tdv) {
    	targetDefaultValues = tdv;
    }

    // Set optional execution properties 
    public void setCommitFrequency(int cf) {
        commitFrequency = cf;
    }
    
    // Execution methods
    // Get list of common source/target columns
    public void retrieveColumnList() throws Exception {
    	logger.info("########################################");
    	logger.info("RETRIEVING COLUMN LIST...");
    	
    	String sourcePrefix = "";
    	if (!(sourceCon.getSchemaName() == null || sourceCon.getSchemaName().equals(""))) {
    		sourcePrefix = sourceCon.getSchemaName() + ".";
    		logger.debug("Prefix for source table: " + sourcePrefix);
    	}
    	
       	String sql;
       	if (sourceQuery == null || sourceQuery.equals("")) {
       		sql = "SELECT * FROM " + sourceCon.getObjectIdentifier(sourceTable);
       	}
       	else {
       		sql = sourceQuery;
       	}
       	
       	// Get source column dictionary
       	sourceDictionaryBean.setSourceQuery(sql);
       	sourceDictionaryBean.retrieveColumns();
       	sourceColumnNames = sourceDictionaryBean.getColumnNames();
       	sourceColumnType = sourceDictionaryBean.getColumnTypes();       	

    	sql = "SELECT * FROM " + targetSchema + "." + targetTable;

       	// Get target column dictionary
       	targetDictionaryBean.setSourceQuery(sql);
       	targetDictionaryBean.retrieveColumns();
       	targetColumnNames = targetDictionaryBean.getColumnNames();
       	targetColumnType = targetDictionaryBean.getColumnTypes();
    	
    	List<String> listName = new ArrayList<String>();
    	List<String> listType = new ArrayList<String>();
        for (int s = 0; s < sourceColumnNames.length; s++) {
            for (int t = 0; t < targetColumnNames.length; t++) {
            	if (sourceColumnNames[s].equalsIgnoreCase(targetColumnNames[t])) {
            		listName.add(targetColumnNames[t]);
            		listType.add(targetColumnType[t]);
            	}
            }
        }
        
        commonColumnNames = new String[listName.size()];
        commonColumnTypes = new String[listType.size()];
        listName.toArray(commonColumnNames);
        listType.toArray(commonColumnTypes);
        
        logger.info("COLUMN LIST RETRIEVED");
        logger.info("########################################");
    }
    
    public void retrieveMappingDefinition() throws Exception {
    	
    	// Load mapping definition file
    	logger.info("LOADING MAP DEFINITION FILE " + mappingDefFile + "...");
    	
    	Document mappingXML = null;
    	
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
		mappingXML = docBuilder.parse(mappingDefFile);
		mappingXML.getDocumentElement().normalize();
		
		// Local variables
		NodeList nList;
		Node nNode;
		Element eElement;
		
		// get source to target column mapping
		nList = mappingXML.getElementsByTagName("columnMapping");
		sourceMapColumns = new String[nList.getLength()];
		targetMapColumns = new String[nList.getLength()];
		for (int i = 0; i < nList.getLength(); i++) {
 			nNode = nList.item(i);
 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 				eElement = (Element)nNode;
 				sourceMapColumns[i] = eElement.getElementsByTagName("source").item(0).getChildNodes().item(0).getNodeValue();
 				targetMapColumns[i] = eElement.getElementsByTagName("target").item(0).getChildNodes().item(0).getNodeValue();
 			}
		}
		
		// get default value to target column mapping
		nList = mappingXML.getElementsByTagName("defaultValue");
		targetDefaultColumns = new String[nList.getLength()];
		targetDefaultValues = new String[nList.getLength()];
		for (int i = 0; i < nList.getLength(); i++) {
 			nNode = nList.item(i);
 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 				eElement = (Element)nNode;
 				targetDefaultColumns[i] = eElement.getElementsByTagName("column").item(0).getChildNodes().item(0).getNodeValue();
 				targetDefaultValues[i] = eElement.getElementsByTagName("value").item(0).getChildNodes().item(0).getNodeValue();
 			}
		}
    	logger.info("LOADED MAP DEFINITION FILE");
    }

    // Execution methods
    // Perform select on source
    public void executeSelect() throws Exception {
    	logger.info("########################################");
    	logger.info("GETTING DATA");
    	
    	String queryText;
    	
    	if (sourceQuery == null || sourceQuery.equals("")) {
    	
	    	queryText = "SELECT ";
	
	    	for (int i = 0; i < commonColumnNames.length; i++) {
	    		if (i > 0) {
	    			queryText += ",";
	    		}
	    		queryText += sourceCon.getColumnIdentifier(commonColumnNames[i]);
	    	}
	    	if (sourceMapColumns!=null) {
	    		for (int i = 0; i < sourceMapColumns.length; i++) {
	    			queryText += "," + sourceMapColumns[i];
	    		}
	    	}
			queryText += " FROM " + sourceCon.getObjectIdentifier(sourceTable);
    	}
    	else {
    		queryText = sourceQuery;
    	}
    	
		logger.info(queryText);
    	
        sourceStmt = sourceCon.getConnection().prepareStatement(queryText);
	    sourceRS = sourceStmt.executeQuery();
	    logger.debug("DATA READY");
	    
        logger.info("GOT DATA");
        logger.info("########################################");
    }

    // Loop on source records and perform inserts
    public void executeInsert() throws Exception {
        logger.info("########################################");
    	logger.info("INSERTING DATA...");

    	targetCon.getConnection().setAutoCommit(false);
        PreparedStatement targetStmt;
        logger.info("Preserve target data = " + preserveDataOption);
        if (!preserveDataOption) {
            logger.info("Truncate table");
            String truncateText = "";
            if (targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")) {
            	truncateText = "DELETE " + targetSchema + "." + targetTable + " ALL";
            }
            else {
	            truncateText = "TRUNCATE TABLE " + targetSchema + "." + targetTable;
	           	if (targetCon.getDatabaseProductName().toUpperCase().contains("DB2")) {
	           		targetCon.closeConnection();
	           		targetCon.openConnection();
	           		truncateText += " IMMEDIATE";
	           	}
            }
            logger.debug(truncateText);
           	targetStmt = targetCon.getConnection().prepareStatement(truncateText);
            targetStmt.executeUpdate();
            targetStmt.close();
            targetCon.getConnection().commit();
            logger.info("Table truncated");
        }
        
        String insertText = "INSERT /*+APPEND*/ INTO " + targetSchema + "." + targetTable + " (";
        for (int i = 0; i < commonColumnNames.length; i++) {
        	if (i > 0) {
        		insertText += ",";
        	}
        	insertText += targetCon.getColumnIdentifier(commonColumnNames[i]);
        }
        
        if (targetMapColumns!=null) {
	       for (int i = 0; i < targetMapColumns.length; i++) {
	    	   insertText += "," + targetMapColumns[i];
	       }
	    }
	       
	    if (targetDefaultColumns!=null) {
	    	for (int i = 0; i < targetDefaultColumns.length; i++) {
	          	insertText += "," + targetDefaultColumns[i];
	        }
	    }
        
	    insertText += ") VALUES (";
	    
	    for (int i = 0; i < commonColumnNames.length; i++) {
	    	if (i > 0) {
	    		insertText = insertText + ",";
	    	}
	    	insertText = insertText + "?";
	    }
	    
	    if (targetMapColumns!=null) {
	    	for (int i = 0; i < targetMapColumns.length; i++) {
	    		insertText += ",?";
	    	}
	    }
	    
	    if (targetDefaultColumns!=null) {
	    	for (int i = 0; i < targetDefaultColumns.length; i++) {
	    		insertText += ",?";
	    	}
	    }
	    
	    insertText = insertText + ")";
	    
	    logger.debug(insertText);
	    logger.debug("Statement prepared");
	    
	    int rowCount = 0;
	    int rowSinceCommit = 0;
	    logger.info("Commit every " + commitFrequency + " rows");
    	targetStmt = targetCon.getConnection().prepareStatement(insertText);
    	targetStmt.setFetchSize(commitFrequency);
    	//List<Object> bufferLine = null;
	    while (sourceRS.next()) {
	    	try {
	    		int position = 0;
	    		
	    		for (int i = 0; i < commonColumnNames.length; i++) {
	    			position++;
	    			try {
	    				if (
	    					commonColumnTypes[i].equalsIgnoreCase("CLOB") &&
	    					targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")
	    				) {
			    			targetStmt.setClob(position, sourceRS.getClob(commonColumnNames[i]));
	    				}
	    				else {
			    			targetStmt.setObject(position, sourceRS.getObject(commonColumnNames[i]));
			    		}
	    			}
	    			catch (Exception e){
	              		if (targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")) {
	              			targetStmt.setNull(position, Types.NULL);
		              	}
		              	else {
		              		targetStmt.setObject(position, null);
		              	}
	    			}
	    		}
	    		
	    		if (sourceMapColumns!=null) {
	    			for (int i = 0; i < sourceMapColumns.length; i++) {
		             	position++;
		              	try {
		              		targetStmt.setObject(position, sourceRS.getObject(sourceMapColumns[i]));
		                }
		                catch (Exception e){
		                	targetStmt.setObject(position, null);
		                }
		            }
	    		}
	            
	            if (targetDefaultValues!=null) {
	            	for (int i = 0; i < targetDefaultValues.length; i++) {
	            		position++;
		                try {
		                	targetStmt.setObject(position, targetDefaultValues[i]);
		                }
		                catch (Exception e){
		                	targetStmt.setObject(position, null);
		                }
		            }
	            }
		    	targetStmt.executeUpdate();
		    	targetStmt.clearParameters();
	        }
	        catch(Exception e) {
	        	logger.error("Unexpected exception, list of column values:");
	        	for (int i = 0; i < commonColumnNames.length; i++) {
	        		try {
	        			logger.error("########################################\n" + commonColumnNames[i] + " ==> " + sourceRS.getObject(commonColumnNames[i]).toString());
				    }
	        		catch(NullPointerException npe) {
	        			logger.error("########################################\n" + commonColumnNames[i]);
			        }
	            }
	            logger.error(e.getMessage());
	            throw e;
	        }
	    	
	    	rowCount++;
	    	rowSinceCommit++;
	    	if (rowSinceCommit==commitFrequency) {
	    		targetCon.getConnection().commit();
	    		rowSinceCommit = 0;
	    		logger.info(rowCount + " rows inserted");
	    	}
	    }
    	targetStmt.close();
        targetCon.getConnection().commit();

	    sourceRS.close();
	    sourceStmt.close();

	    logger.info(rowCount + " rows totally inserted");
	    logger.info("INSERT COMPLETED");
	    logger.info("########################################");
    }
}
