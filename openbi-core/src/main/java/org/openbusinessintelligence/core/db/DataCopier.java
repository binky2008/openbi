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
public class DataCopier {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataCopier.class);

    // Declarations of bean properties
    private StatementBuilder statement = null;
	// Source properties
    private DBConnection sourceCon = null;
    private String sourceSchema = "";
    private String sourceTable = "";
    private String sourceQuery = "";
    private String[] queryParameters = null;
    private String[] sourceColumnNames = null;
    private String[] sourceColumnType = null;
    private String[] sourceColumnTypeAttribute = null;
    private int[] sourceColumnLength = null;

    // Target properties
    private DBConnection targetCon = null;
    private String targetSchema = "";
    private String targetTable = "";
    private boolean preserveDataOption = false;
    private String[] targetColumnNames = null;
    private String[] targetColumnType = null;
    private String[] targetColumnTypeAttribute = null;
    private int[] targetColumnLength = null;
    
    // Mapping properties
    private String mappingDefFile = "";
    private String[] sourceMapColumns = null;
    private String[] targetMapColumns = null;
    private String[] targetDefaultColumns = null;
    private String[] targetDefaultValues = null;
    
    // Execution properties
    private int commitFrequency;

    // Declarations of internally used variables
	DictionaryExtractor sourceDictionaryBean;
	DictionaryExtractor targetDictionaryBean;
    private String[] commonColumnNames = null;
    private String[] sourceCommonColumnTypes = null;
    private String[] sourceCommonColumnTypeAttribute = null;
    private String[] targetCommonColumnTypes = null;
    private String[] targetCommonColumnTypeAttribute = null;
    private int[] sourceCommonColumnLength = null;
    private int[] targetCommonColumnLength = null;
    private ResultSet sourceRS = null;
    private PreparedStatement sourceStmt= null;
    private int lengthMultiplier = 1;
    
    // Constructor
    public DataCopier() {
        super();
        sourceDictionaryBean = new DictionaryExtractor();
        targetDictionaryBean = new DictionaryExtractor();
    }

    // Set source properties methods
    public void setSourceConnection(DBConnection property) {
    	sourceCon = property;
    	sourceDictionaryBean.setSourceConnection(sourceCon);
    }

    public void setSourceSchema(String ta) {
        sourceSchema = ta;
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
    public void setTargetConnection(DBConnection property) {
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
    	logger.debug("Source schema: " + sourceSchema + " - Source table: " + sourceTable);
    	
       	if (sourceQuery == null || sourceQuery.equals("")) {
       		sourceDictionaryBean.setSourceSchema(sourceSchema);
           	sourceDictionaryBean.setSourceTable(sourceTable);
       	}
       	else {
           	sourceDictionaryBean.setSourceQuery(sourceQuery);
       	}
       	
       	// Get source column dictionary
       	sourceDictionaryBean.retrieveColumns();
       	sourceColumnNames = sourceDictionaryBean.getColumnNames();
       	sourceColumnType = sourceDictionaryBean.getColumnTypes();     
       	sourceColumnTypeAttribute = sourceDictionaryBean.getColumnTypeAttribute();
       	sourceColumnLength = sourceDictionaryBean.getColumnLength();


       	// Get target column dictionary
    	logger.debug("Target schema: " + targetSchema + " - Target table: " + targetTable);
       	targetDictionaryBean.setSourceSchema(targetSchema);
       	targetDictionaryBean.setSourceTable(targetTable);
       	targetDictionaryBean.retrieveColumns();
       	targetColumnNames = targetDictionaryBean.getColumnNames();
       	targetColumnType = targetDictionaryBean.getColumnTypes();
       	targetColumnTypeAttribute = targetDictionaryBean.getColumnTypeAttribute();
       	targetColumnLength = targetDictionaryBean.getColumnLength();

        statement = new StatementBuilder();
        statement.setProductName(targetCon.getDatabaseProductName().toUpperCase());
    	
    	List<String> listName = new ArrayList<String>();
    	List<String> listSourceType = new ArrayList<String>();
    	List<String> listSourceTypeAttribute = new ArrayList<String>();
    	List<Integer> listSourceLength = new ArrayList<Integer>();
    	List<String> listTargetType = new ArrayList<String>();
    	List<String> listTargetTypeAttribute = new ArrayList<String>();
    	List<Integer> listTargetLength = new ArrayList<Integer>();
        for (int s = 0; s < sourceColumnNames.length; s++) {
            for (int t = 0; t < targetColumnNames.length; t++) {
            	if (
            		sourceColumnNames[s].equalsIgnoreCase(targetColumnNames[t]) &&
            		statement.getColumnUsable(targetColumnType[t])
            	) {
            		listName.add(targetColumnNames[t]);
            		listSourceType.add(sourceColumnType[s]);
            		listSourceTypeAttribute.add(sourceColumnTypeAttribute[s]);
            		listSourceLength.add(sourceColumnLength[s]);
            		listTargetType.add(targetColumnType[t]);
            		listTargetTypeAttribute.add(targetColumnTypeAttribute[t]);
            		listTargetLength.add(targetColumnLength[t]);
            	}
            }
        }
        
        commonColumnNames = new String[listName.size()];
        sourceCommonColumnTypes = new String[listSourceType.size()];
        sourceCommonColumnTypeAttribute = new String[listSourceTypeAttribute.size()];
        sourceCommonColumnLength = new int[listSourceLength.size()];
        targetCommonColumnTypes = new String[listTargetType.size()];
        targetCommonColumnTypeAttribute = new String[listTargetTypeAttribute.size()];
        targetCommonColumnLength = new int[listTargetLength.size()];
        Integer[] sourceCommonColumnLengthInteger = new Integer[listSourceLength.size()];
        Integer[] targetCommonColumnLengthInteger = new Integer[listTargetLength.size()];
        
        listName.toArray(commonColumnNames);
        listSourceType.toArray(sourceCommonColumnTypes);
        listSourceTypeAttribute.toArray(sourceCommonColumnTypeAttribute);
        listSourceLength.toArray(sourceCommonColumnLengthInteger);
        listTargetType.toArray(targetCommonColumnTypes);
        listTargetTypeAttribute.toArray(targetCommonColumnTypeAttribute);
        listTargetLength.toArray(targetCommonColumnLengthInteger);
        
        for (int i = 0; i < sourceCommonColumnLength.length; i++) {
        	sourceCommonColumnLength[i] = sourceCommonColumnLengthInteger[i];
        }
        for (int i = 0; i < targetCommonColumnLength.length; i++) {
        	targetCommonColumnLength[i] = targetCommonColumnLengthInteger[i];
        }
        
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
	    		
	    		/*if (sourceCon.getDatabaseProductName().toUpperCase().contains("HIVE")) {
	    			
	    		}*/
	    		
		    	if (
			    	!sourceCommonColumnTypes[i].toUpperCase().contains("VAR") &&
			    	!sourceCommonColumnTypeAttribute[i].toUpperCase().contains("BIT") &&
			    	(
			    		sourceCommonColumnTypes[i].toUpperCase().contains("CHAR") ||
			    		sourceCommonColumnTypes[i].toUpperCase().contains("GRAPHIC") ||
			    		(
			    			sourceCommonColumnTypes[i].toUpperCase().contains("BINARY") &&
			    			!sourceCon.getDatabaseProductName().toUpperCase().contains("HIVE") &&
			    			!sourceCon.getDatabaseProductName().toUpperCase().contains("HSQL") &&
			    			!sourceCon.getDatabaseProductName().toUpperCase().contains("INFORMIX") &&
			    			!sourceCon.getDatabaseProductName().toUpperCase().contains("VERTICA")
			    		)
			    	)
			    ) {
		    		if (
		    			sourceCon.getDatabaseProductName().toUpperCase().contains("FIREBIRD") ||
		    			sourceCon.getDatabaseProductName().toUpperCase().contains("INFORMIX")
			    	) {
			    		queryText += "TRIM(TRAILING FROM " + sourceCon.getColumnIdentifier(commonColumnNames[i]) + ") AS " + sourceCon.getColumnIdentifier(commonColumnNames[i]);
		    		}
		    		else {
			    		queryText += "RTRIM(" + sourceCon.getColumnIdentifier(commonColumnNames[i]) + ") AS " + sourceCon.getColumnIdentifier(commonColumnNames[i]);
		    		}
			    }
		    	else if (
	    	    	sourceCon.getDatabaseProductName().toUpperCase().contains("DERBY") &&
	    	    	sourceCommonColumnTypes[i].toUpperCase().contains("XML")
	    	    ) {
		    		queryText += "XMLSERIALIZE(" + sourceCon.getColumnIdentifier(commonColumnNames[i]) + " AS CLOB) AS " + sourceCon.getColumnIdentifier(commonColumnNames[i]);
	    		}
	    		else if (
		    	    sourceCon.getDatabaseProductName().toUpperCase().contains("TERADATA") &&
		    	    sourceCommonColumnTypes[i].equalsIgnoreCase("N")
		    	) {
			    	queryText += "CAST(" + sourceCon.getColumnIdentifier(commonColumnNames[i]) + " AS DECIMAL) AS " + sourceCon.getColumnIdentifier(commonColumnNames[i]);
		    	}
	    		else {
		    		queryText += sourceCon.getColumnIdentifier(commonColumnNames[i]);
	    		}
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
        // Build table identifier
        String tableIdentifier = "";
    	if (!(targetSchema == null || targetSchema.equals(""))) {
    		tableIdentifier = targetSchema + "." + targetTable;
    	}
    	else {
    		tableIdentifier = targetTable;
    	}
        
        // Initialize statement string factory
        statement = new StatementBuilder();
        statement.setProductName(targetCon.getDatabaseProductName().toUpperCase());
        statement.setTargetSchema(targetSchema);
        statement.setTargetTable(targetTable);
        String emptyText = statement.getEmptyTable();
    	
    	// Empty table if data are not to be preserved
        logger.info("Preserve target data = " + preserveDataOption);
        if (
        	!preserveDataOption &&
        	!targetCon.getDatabaseProductName().toUpperCase().contains("IMPALA")
        ) {
            logger.info("Truncate table");
            
            logger.debug(emptyText);
           	targetStmt = targetCon.getConnection().prepareStatement(emptyText);
            targetStmt.executeUpdate();
            targetStmt.close();
            if (!targetCon.getDatabaseProductName().toUpperCase().contains("HIVE")) {
                targetCon.getConnection().commit();
            }
            logger.info("Table truncated");
        }
        
        // Build insert statement	    
        String insertText = "INSERT ";
        String insertParameter = "?";
        if (targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE")) {
        	insertText += "/*+APPEND*/ ";
        }
        insertText += "INTO ";
        if (targetCon.getDatabaseProductName().toUpperCase().contains("HIVE")) {
        	insertText += "TABLE ";
        }
        insertText += tableIdentifier + " ";
	        if (!targetCon.getDatabaseProductName().toUpperCase().contains("HIVE")) {
	        insertText += " (";
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
		    insertText += ") ";
        }
	    insertText += "VALUES (";
	    
	    for (int i = 0; i < commonColumnNames.length; i++) {
	    	insertParameter = "?";
	    	if (i > 0) {
	    		insertText = insertText + ",";
	    	}
	    	
	    	if (
		    	//targetCon.getDatabaseProductName().toUpperCase().contains("HIVE") ||
	    		targetCon.getDatabaseProductName().toUpperCase().contains("IMPALA")
	    	) {
		    	if (targetCommonColumnTypes[i].toUpperCase().contains("TINYINT")) {
			    	insertParameter = "CAST(? AS TINYINT)";
			    }
		    	else if (targetCommonColumnTypes[i].toUpperCase().contains("SMALLINT")) {
				    insertParameter = "CAST(? AS SMALLINT)";
				}
		    	else if (targetCommonColumnTypes[i].toUpperCase().contains("BIGINT")) {
				    insertParameter = "CAST(? AS BIGINT)";
				}
		    	else if (targetCommonColumnTypes[i].toUpperCase().contains("INT")) {
				    insertParameter = "CAST(? AS INT)";
				}
		    	else if (targetCommonColumnTypes[i].toUpperCase().contains("DECIMAL")) {
				    insertParameter = "CAST(? AS DECIMAL)";
				}
		    	else if (targetCommonColumnTypes[i].toUpperCase().contains("DOUBLE")) {
				    insertParameter = "CAST(? AS DOUBLE)";
				}
		    	else if (targetCommonColumnTypes[i].toUpperCase().contains("FLOAT")) {
				    insertParameter = "CAST(? AS FLOAT)";
				}
		    	else if (targetCommonColumnTypes[i].toUpperCase().contains("REAL")) {
				    insertParameter = "CAST(? AS REAL)";
				}
	    	}
	    	
	    	// Remove trailing blanks from strings derived from fixed length column types
	    	if (
	    		!targetCommonColumnTypes[i].toUpperCase().contains("BLOB") &&
	    		!targetCommonColumnTypeAttribute[i].toUpperCase().contains("BIT") &&
	    		!sourceCommonColumnTypes[i].toUpperCase().contains("VAR") &&
	    		!sourceCommonColumnTypes[i].toUpperCase().contains("FLOAT") &&
	    		!sourceCommonColumnTypes[i].toUpperCase().contains("DOUBLE") &&
	    		(
	    			sourceCommonColumnTypes[i].toUpperCase().contains("CHAR") ||
	    			sourceCommonColumnTypes[i].toUpperCase().contains("GRAPHIC") ||
	    			sourceCommonColumnTypes[i].toUpperCase().contains("BINARY") ||
	    			(
	    				sourceCon.getDatabaseProductName().toUpperCase().contains("TERADATA") &&
	    				sourceCommonColumnTypes[i].toUpperCase().contains("BYTE") &&
	    				!sourceCommonColumnTypes[i].toUpperCase().contains("INT")
	    			)
	    		) &&
	    		!(
		    		(
		    			targetCommonColumnTypes[i].toUpperCase().contains("BINARY") &&
		    			(
		    				targetCon.getDatabaseProductName().toUpperCase().contains("HSQL") ||
		    				targetCon.getDatabaseProductName().toUpperCase().contains("INFORMIX") ||
		    				targetCon.getDatabaseProductName().toUpperCase().contains("VERTICA")
		    			)
		    		) ||
		    		(
			    		targetCommonColumnTypes[i].toUpperCase().contains("BYTE") &&
			    		targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")
			    	) ||
		    		(
				    	(
				    		sourceCon.getDatabaseProductName().toUpperCase().contains("SQL ANYWHERE") ||
				    		sourceCon.getDatabaseProductName().toUpperCase().contains("VERTICA")
				    	) &&
				    	targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")
				    ) ||
		    		(
		    			targetCommonColumnTypes[i].toUpperCase().contains("BYTEA") &&
		    			targetCon.getDatabaseProductName().toUpperCase().contains("POSTGRES")
		    		) ||
		    		(
				    	sourceCommonColumnTypes[i].toUpperCase().contains("BINARY") &&
				    	sourceCon.getDatabaseProductName().toUpperCase().contains("MYSQL") &&
				    	targetCon.getDatabaseProductName().toUpperCase().contains("NETEZZA")
				    ) ||
		    		(
					    (
					    	targetCommonColumnTypes[i].toUpperCase().contains("CLOB") ||
					    	targetCommonColumnTypes[i].toUpperCase().contains("TEXT")
					    ) &&
					    targetCon.getDatabaseProductName().toUpperCase().contains("INFORMIX")
					)
		    	)
	    	) {
	    		if (
	    			targetCon.getDatabaseProductName().toUpperCase().contains("FIREBIRD") ||
	    			targetCon.getDatabaseProductName().toUpperCase().contains("INFORMIX")
	    		) {
	    			insertParameter = "TRIM(TRAILING FROM ?)";
	    		}
	    		else if (targetCon.getDatabaseProductName().toUpperCase().contains("HIVE")) {
		    		insertParameter = "?";
		    	}
	    		else {
		    		insertParameter = "RTRIM(?)";
	    		}
	    	}
	   	   		
   	   		// Double column length in case source type is binary and target is Netezza varchar
   	   		if (
   	   			sourceCommonColumnTypeAttribute[i].toUpperCase().contains("BIT") ||
   	   			sourceCommonColumnTypes[i].toUpperCase().contains("BINARY") ||
		    	sourceCommonColumnTypes[i].toUpperCase().contains("BYTE")
   	   		) {
   	   			if (
   	   				targetCon.getDatabaseProductName().toUpperCase().contains("DERBY")
   	   			) {
   	   	   			lengthMultiplier = 2;
   	   			}
   	   			else {
   	   				lengthMultiplier = 4;
   	   			}
   	   		}
   	   		else {
   	   			lengthMultiplier = 1;
   	   		}
	    	
   	   		// Cut strings to target length
	    	if (
	    		targetCommonColumnTypes[i].toUpperCase().contains("CHAR") &&
	    		!targetCommonColumnTypeAttribute[i].toUpperCase().contains("BIT") &&
	    		targetCommonColumnLength[i] > 0 &&
	    		targetCommonColumnLength[i] < sourceCommonColumnLength[i] * lengthMultiplier
	    	) {
	    		if (
				    sourceCommonColumnTypes[i].toUpperCase().contains("BINARY") &&
				    sourceCon.getDatabaseProductName().toUpperCase().contains("MYSQL") &&
				    targetCon.getDatabaseProductName().toUpperCase().contains("NETEZZA")
	    		) {
	    			
	    		}
	    		else if (targetCon.getDatabaseProductName().toUpperCase().contains("FIREBIRD")) {
		    		insertParameter = "SUBSTRING(" + insertParameter + " FROM 1 FOR " + targetCommonColumnLength[i] + ")";
	    		}
	    		else if (
	    			targetCon.getDatabaseProductName().toUpperCase().contains("DB2") ||
	    			targetCon.getDatabaseProductName().toUpperCase().contains("DERBY") ||
	    			targetCon.getDatabaseProductName().toUpperCase().contains("INFORMIX") ||
	    			targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE") ||
	    			targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")
	    		) {
		    		insertParameter = "SUBSTR(" + insertParameter + ", 1, " + targetCommonColumnLength[i] + ")";
	    		}
	    		else {
		    		insertParameter = "SUBSTRING(" + insertParameter + ", 1, " + targetCommonColumnLength[i] + ")";
	    		}
	    	}
	    	
	    	// Special casts and conversions
	    	if (
              	targetCon.getDatabaseProductName().toUpperCase().contains("MICROSOFT") &&
              	targetCommonColumnTypes[i].toUpperCase().contains("BINARY")
            ) {
		    	insertText = insertText + "CONVERT(VARBINARY," + insertParameter + ")";
	    	}
	    	else if (
	            targetCon.getDatabaseProductName().toUpperCase().contains("POSTGRESQL") &&
	            targetCommonColumnTypes[i].toUpperCase().contains("BIT")
	        ) {
			    insertText = insertText + "CAST(" + insertParameter + " AS VARBIT)";
		    }
	    	else if (
	    		targetCon.getDatabaseProductName().toUpperCase().contains("DERBY") &&
	    		targetCommonColumnTypes[i].toUpperCase().contains("XML")
    	    ) {
	    		insertText = insertText + "XMLPARSE (DOCUMENT CAST (" + insertParameter + " AS CLOB) PRESERVE WHITESPACE)";
    		}
	    	else {
		    	insertText = insertText + insertParameter;
	    	}
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
    	
    	DataManipulator dataManipulate = new DataManipulator();
    	dataManipulate.setSourceProductName(sourceCon.getDatabaseProductName().toUpperCase());
    	dataManipulate.setTargetProductName(targetCon.getDatabaseProductName().toUpperCase());
    	dataManipulate.setResultSet(sourceRS);
    	dataManipulate.setStatement(targetStmt);
    	
	    while (sourceRS.next()) {
	    	try {
	    		int position = 0;
	    		
	    		for (int i = 0; i < commonColumnNames.length; i++) {
	    			position++;
	    			dataManipulate.setColumnName(commonColumnNames[i]);
	    			dataManipulate.setPosition(position);
	    			dataManipulate.setSourceType(sourceCommonColumnTypes[i]);
	    			dataManipulate.setSourceTypeAttribute(sourceCommonColumnTypeAttribute[i]);
	    			dataManipulate.setTargetType(targetCommonColumnTypes[i]);
	    			dataManipulate.setTargetTypeAttribute(targetCommonColumnTypeAttribute[i]);
	    			dataManipulate.setTargetLength(targetColumnLength[i]);
	    			dataManipulate.copyObject();
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
	        			logger.error(commonColumnNames[i] + ": " + sourceCommonColumnTypes[i] + " => " + targetCommonColumnTypes[i] + " = " + String.valueOf(sourceRS.getObject(commonColumnNames[i])));
				    }
	        		catch(Exception ee) {
	        			logger.error(commonColumnNames[i] + ": " + sourceCommonColumnTypes[i] + " => " + targetCommonColumnTypes[i]);
			        }
	            }
	            logger.error(e.getMessage());
	            throw e;
	        }
	    	
	    	rowCount++;
	    	rowSinceCommit++;
	    	if (rowSinceCommit==commitFrequency) {
	    		if (
	    			!targetCon.getDatabaseProductName().toUpperCase().contains("HIVE") &&
	    			!targetCon.getDatabaseProductName().toUpperCase().contains("IMPALA")
	    		) {
		    		targetCon.getConnection().commit();
	    		}
	    		rowSinceCommit = 0;
	    		logger.info(rowCount + " rows inserted");
	    	}
	    }
    	targetStmt.close();
		if (
    		!targetCon.getDatabaseProductName().toUpperCase().contains("HIVE") &&
    		!targetCon.getDatabaseProductName().toUpperCase().contains("IMPALA")
    	) {
    		targetCon.getConnection().commit();
		}

	    sourceRS.close();
	    sourceStmt.close();

	    logger.info(rowCount + " rows totally inserted");
	    logger.info("INSERT COMPLETED");
	    logger.info("########################################");
    }
    
    public static String getString(Clob clb) throws IOException, SQLException {
    	if (clb == null) {
    		return  "";
    	}
    	else {
    		StringBuffer stringBuffer = new StringBuffer();
    		String strng;
    		BufferedReader bufferReader = new BufferedReader(clb.getCharacterStream());
    		while ((strng=bufferReader .readLine())!=null) {
    			stringBuffer.append(strng);
    		}
    		return stringBuffer.toString();
    	}        
    }
}
