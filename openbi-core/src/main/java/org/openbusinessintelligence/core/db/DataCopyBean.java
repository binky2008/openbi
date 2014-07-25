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
    private StatementBean statement = null;
	// Source properties
    private ConnectionBean sourceCon = null;
    private String sourceSchema = "";
    private String sourceTable = "";
    private String sourceQuery = "";
    private String[] queryParameters = null;
    private String[] sourceColumnNames = null;
    private String[] sourceColumnType = null;
    private String[] sourceColumnTypeAttribute = null;

    // Target properties
    private ConnectionBean targetCon = null;
    private String targetSchema = "";
    private String targetTable = "";
    private boolean preserveDataOption = false;
    private String[] targetColumnNames = null;
    private String[] targetColumnType = null;
    private String[] targetColumnTypeAttribute = null;
    
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
    private String[] sourceCommonColumnTypes = null;
    private String[] sourceCommonColumnTypeAttribute = null;
    private String[] targetCommonColumnTypes = null;
    private String[] targetCommonColumnTypeAttribute = null;
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


       	// Get target column dictionary
    	logger.debug("Target schema: " + targetSchema + " - Target table: " + targetTable);
       	targetDictionaryBean.setSourceSchema(targetSchema);
       	targetDictionaryBean.setSourceTable(targetTable);
       	targetDictionaryBean.retrieveColumns();
       	targetColumnNames = targetDictionaryBean.getColumnNames();
       	targetColumnType = targetDictionaryBean.getColumnTypes();
       	targetColumnTypeAttribute = targetDictionaryBean.getColumnTypeAttribute();

        statement = new StatementBean();
        statement.setProductName(targetCon.getDatabaseProductName().toUpperCase());
    	
    	List<String> listName = new ArrayList<String>();
    	List<String> listSourceType = new ArrayList<String>();
    	List<String> listSourceTypeAttribute = new ArrayList<String>();
    	List<String> listTargetType = new ArrayList<String>();
    	List<String> listTargetTypeAttribute = new ArrayList<String>();
        for (int s = 0; s < sourceColumnNames.length; s++) {
            for (int t = 0; t < targetColumnNames.length; t++) {
            	if (
            		sourceColumnNames[s].equalsIgnoreCase(targetColumnNames[t]) &&
            		statement.getColumnUsable(targetColumnType[t])
            	) {
            		listName.add(targetColumnNames[t]);
            		listSourceType.add(sourceColumnType[t]);
            		listSourceTypeAttribute.add(sourceColumnTypeAttribute[t]);
            		listTargetType.add(targetColumnType[t]);
            		listTargetTypeAttribute.add(targetColumnTypeAttribute[t]);
            	}
            }
        }
        
        commonColumnNames = new String[listName.size()];
        sourceCommonColumnTypes = new String[listSourceType.size()];
        sourceCommonColumnTypeAttribute = new String[listSourceTypeAttribute.size()];
        targetCommonColumnTypes = new String[listTargetType.size()];
        targetCommonColumnTypeAttribute = new String[listTargetTypeAttribute.size()];
        listName.toArray(commonColumnNames);
        listSourceType.toArray(sourceCommonColumnTypes);
        listTargetType.toArray(targetCommonColumnTypes);
        listTargetTypeAttribute.toArray(targetCommonColumnTypeAttribute);
        
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
	    		if (
	    	    	sourceCon.getDatabaseProductName().toUpperCase().contains("DERBY") &&
	    	    	sourceCommonColumnTypes[i].toUpperCase().contains("XML")
	    	    ) {
		    		queryText += sourceCon.getColumnIdentifier("XMLSERIALIZE(" + commonColumnNames[i] + " AS CLOB) AS " + commonColumnNames[i]);
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
        statement = new StatementBean();
        statement.setProductName(targetCon.getDatabaseProductName().toUpperCase());
        statement.setTargetSchema(targetSchema);
        statement.setTargetTable(targetTable);
        String emptyText = statement.getEmptyTable();
    	
    	// Empty table if data are not to be preserved
        logger.info("Preserve target data = " + preserveDataOption);
        if (!preserveDataOption) {
            logger.info("Truncate table");
            
            logger.debug(emptyText);
           	targetStmt = targetCon.getConnection().prepareStatement(emptyText);
            targetStmt.executeUpdate();
            targetStmt.close();
            targetCon.getConnection().commit();
            logger.info("Table truncated");
        }
        
        // Build insert statement	    
        String insertText = "INSERT ";
        if (targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE")) {
        	insertText += "/*+APPEND*/ ";
        }
         insertText += "INTO " + tableIdentifier + " (";
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
	    	if (
              	targetCon.getDatabaseProductName().toUpperCase().contains("MICROSOFT") &&
              	targetCommonColumnTypes[i].toUpperCase().contains("BINARY")
            ) {
		    	insertText = insertText + "CONVERT(VARBINARY,?)";
	    	}
	    	else if (
	    		targetCon.getDatabaseProductName().toUpperCase().contains("DERBY") &&
	    		targetCommonColumnTypes[i].toUpperCase().contains("XML")
    	    ) {
	    		insertText = insertText + "XMLPARSE (DOCUMENT CAST (? AS CLOB) PRESERVE WHITESPACE)";
    		}
	    	else {
		    	insertText = insertText + "?";
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
    	//List<Object> bufferLine = null;
    	Object object = null;
    	String className = "";
    	
    	DataManipulationBean dataManipulate = new DataManipulationBean();
    	dataManipulate.setSourceProductName(sourceCon.getDatabaseProductName().toUpperCase());
    	dataManipulate.setTargetProductName(targetCon.getDatabaseProductName().toUpperCase());
    	
	    while (sourceRS.next()) {
	    	try {
	    		int position = 0;
	    		
	    		for (int i = 0; i < commonColumnNames.length; i++) {
	    			position++;
	    			dataManipulate.setPosition(position);
	    			dataManipulate.setSourceType(sourceCommonColumnTypes[i]);
	    			dataManipulate.setSourceTypeAttribute(sourceCommonColumnTypeAttribute[i]);
	    			dataManipulate.setTargetType(targetCommonColumnTypes[i]);
	    			dataManipulate.setTargetTypeAttribute(targetCommonColumnTypeAttribute[i]);
	    			try {
	    				object = sourceRS.getObject(commonColumnNames[i]);
	    				if (targetCommonColumnTypes[i].contains("SDO")) {
	    					logger.debug("!!!!!!!!!!!!!!! ORACLE SDO TYPE !!!!!!!!!!!!!!!");
	    					targetStmt.setNull(position, Types.NULL);
	    				}
	    				else if (object == null) {
		    				targetStmt.setNull(position, dataManipulate.getSQLType());
		    			}
	    				else {
		    				className = object.getClass().getName();
	    					if (className.contains("BigInteger")) {
	    						targetStmt.setBigDecimal(position, sourceRS.getBigDecimal(commonColumnNames[i]));
	    					}
	    					else if (
						    	sourceCommonColumnTypes[i].contains("TIMESTAMP") &&
			    				sourceCon.getDatabaseProductName().toUpperCase().contains("ORACLE")
						    ) {
	    						targetStmt.setTimestamp(position, sourceRS.getTimestamp(commonColumnNames[i]));
						    }
	    					else if (
			    				sourceCommonColumnTypes[i].toUpperCase().contains("CLOB") &&
			    				sourceCon.getDatabaseProductName().toUpperCase().contains("DB2") &&
			    				(
			    					targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE") ||
			    					targetCon.getDatabaseProductName().toUpperCase().contains("INFORMIX")
			    				)
			    			) {
					    		targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
			    			}
	    					else if (
						    	(
						    		sourceCommonColumnTypes[i].toUpperCase().contains("LOB") ||
						    		sourceCommonColumnTypes[i].toUpperCase().contains("XML") ||
						    		sourceCommonColumnTypes[i].toUpperCase().contains("LONG")
				    			) &&
						    	sourceCon.getDatabaseProductName().toUpperCase().contains("DERBY")
						    ) {
		    					// Cannot handle LOBs and LONGs in Apache Derby
		    					targetStmt.setNull(position, dataManipulate.getSQLType());
						    }
	    					else if (
					    		sourceCommonColumnTypes[i].toUpperCase().contains("XML") &&
				    			(
					    			targetCon.getDatabaseProductName().toUpperCase().contains("DB2") ||
					    			targetCon.getDatabaseProductName().toUpperCase().contains("DERBY") ||
				    				targetCon.getDatabaseProductName().toUpperCase().contains("MYSQL") ||
				    				targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE") ||
				    				targetCon.getDatabaseProductName().toUpperCase().contains("POSTGRES") ||
				    				targetCon.getDatabaseProductName().toUpperCase().contains("MICROSOFT") ||
				    				targetCon.getDatabaseProductName().toUpperCase().contains("H2") ||
				    				targetCon.getDatabaseProductName().toUpperCase().contains("HSQL") ||
				    				targetCon.getDatabaseProductName().toUpperCase().contains("ANYWHERE")
				    			)
				    		) {
						    	targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
				    		}
	    					else if (
							    targetCommonColumnTypes[i].equalsIgnoreCase("DECFLOAT") &&
						    	targetCon.getDatabaseProductName().toUpperCase().contains("DB2")
						    ) {
								targetStmt.setFloat(position, sourceRS.getFloat(commonColumnNames[i]));
						    }
	    					else if (
						    	sourceCommonColumnTypes[i].equalsIgnoreCase("BLOB") &&
					    		targetCommonColumnTypes[i].equalsIgnoreCase("BLOB") &&
					    		targetCon.getDatabaseProductName().toUpperCase().contains("FIREBIRD")
					    	) {
							    targetStmt.setBytes(position, sourceRS.getBytes(commonColumnNames[i]));
					    	}
	    					else if (
				    			targetCommonColumnTypes[i].equalsIgnoreCase("BLOB") &&
				    			targetCon.getDatabaseProductName().toUpperCase().contains("FIREBIRD")
				    		) {
						    	targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
				    		}
	    					else if (
			    				targetCommonColumnTypes[i].equalsIgnoreCase("TEXT") &&
			    				targetCon.getDatabaseProductName().toUpperCase().contains("MICROSOFT")
			    			) {
					    		targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
			    			}
	    					else if (
				    			targetCommonColumnTypes[i].equalsIgnoreCase("TEXT") &&
				    			targetCon.getDatabaseProductName().toUpperCase().contains("INFORMIX")
				    		) {
						    	targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
				    		}
	    					else if (
			    				targetCommonColumnTypes[i].toUpperCase().contains("LONG") &&
			    				targetCon.getDatabaseProductName().toUpperCase().contains("ANYWHERE")
			    			) {
					    		targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
			    			}
	    					else if (
				    			targetCommonColumnTypes[i].equalsIgnoreCase("CLOB") &&
				    			targetCon.getDatabaseProductName().toUpperCase().contains("DB2")
				    		) {
						    	targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
				    		}
	    					else if (
				    			targetCommonColumnTypes[i].equalsIgnoreCase("CLOB") &&
				    			targetCon.getDatabaseProductName().toUpperCase().contains("DERBY")
				    		) {
						    	targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
				    		}
	    					else if (
			    				targetCommonColumnTypes[i].equalsIgnoreCase("CLOB") &&
			    				targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE")
			    			) {
					    		targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
			    			}
	    					else if (
		    					targetCommonColumnTypes[i].equalsIgnoreCase("CLOB") &&
		    					targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")
		    				) {
				    			targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
		    				}
	    					else if (
			    				targetCommonColumnTypes[i].equalsIgnoreCase("BYTE") &&
			    				targetCon.getDatabaseProductName().toUpperCase().contains("INFORMIX")
			    			) {
					    		targetStmt.setBinaryStream(position, sourceRS.getBinaryStream(commonColumnNames[i]));
			    			}
	    					else if (
					    		targetCommonColumnTypes[i].equalsIgnoreCase("BLOB") &&
					    		targetCon.getDatabaseProductName().toUpperCase().contains("DB2")
					    	) {
							    targetStmt.setBinaryStream(position, sourceRS.getBinaryStream(commonColumnNames[i]));
					    	}
	    					else if (
				    			targetCommonColumnTypes[i].equalsIgnoreCase("BLOB") &&
				    			targetCon.getDatabaseProductName().toUpperCase().contains("ORACLE")
				    		) {
						    	targetStmt.setBinaryStream(position, sourceRS.getBinaryStream(commonColumnNames[i]));
				    		}
	    					else if (
					    		targetCommonColumnTypes[i].toUpperCase().contains("VARBINARY") &&
					    		targetCon.getDatabaseProductName().toUpperCase().contains("VERTICA")
					    	) {
							    targetStmt.setBinaryStream(position, sourceRS.getBinaryStream(commonColumnNames[i]));
					    	}
	    					else if (
					    		targetCommonColumnTypes[i].equalsIgnoreCase("BLOB") &&
					    		targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA")
					    	) {
							    targetStmt.setBytes(position, sourceRS.getBytes(commonColumnNames[i]));
					    	}
	    					else if (
					    		targetCommonColumnTypes[i].equalsIgnoreCase("BYTEA") &&
					    		targetCon.getDatabaseProductName().toUpperCase().contains("POSTGRES")
					    	) {
							    targetStmt.setBytes(position, sourceRS.getBytes(commonColumnNames[i]));
					    	}
	    					else if (
								sourceCommonColumnTypes[i].contains("BLOB") &&
								targetCommonColumnTypes[i].contains("VARCHAR") &&
								targetCon.getDatabaseProductName().toUpperCase().contains("NETEZZA")
							) {
	    						targetStmt.setBytes(position, sourceRS.getBytes(commonColumnNames[i]));
							}
	    					else if (
						    	targetCommonColumnTypes[i].contains("VARCHAR") &&
						    	targetCon.getDatabaseProductName().toUpperCase().contains("NETEZZA")
						    ) {
	    							targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
						    }
	    					else if (
							    targetCommonColumnTypes[i].toUpperCase().contains("VARCHAR") &&
							    targetCon.getDatabaseProductName().toUpperCase().contains("VERTICA")
							) {
		    					targetStmt.setString(position, sourceRS.getString(commonColumnNames[i]));
							}
	    					else if (
	    						targetCommonColumnTypeAttribute[i].toUpperCase().contains("FOR BIT DATA") &&
								targetCon.getDatabaseProductName().toUpperCase().contains("DERBY")
							) {
			    				targetStmt.setBinaryStream(position, sourceRS.getBinaryStream(commonColumnNames[i]));
							}
	    					else if (
			    				sourceCommonColumnTypes[i].toUpperCase().equalsIgnoreCase("BIT") &&
								targetCon.getDatabaseProductName().toUpperCase().contains("DERBY")
							) {
				    			targetStmt.setInt(position, sourceRS.getInt(commonColumnNames[i]));
							}
		    				else {
				    			targetStmt.setObject(position, sourceRS.getObject(commonColumnNames[i]));
				    		}
	    					
	    				}
	    			}
	    			catch (Exception e){
	    				logger.error(commonColumnNames[i] + ": " + sourceCommonColumnTypes[i] + " => " + targetCommonColumnTypes[i] + " = " + object + "\n" + e.getMessage());
	    				e.printStackTrace();
	              		if (
	              			targetCon.getDatabaseProductName().toUpperCase().contains("TERADATA") ||
	              			(
	    	              		targetCon.getDatabaseProductName().toUpperCase().contains("MICROSOFT") &&
	    	              		targetCommonColumnTypes[i].equalsIgnoreCase("VARBINARY")
	              			)
	              		) {
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
	        			logger.error(commonColumnNames[i] + ": " + sourceCommonColumnTypes[i] + " => " + targetCommonColumnTypes[i] + " Class: " + className + " = " + sourceRS.getObject(commonColumnNames[i]).toString());
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
