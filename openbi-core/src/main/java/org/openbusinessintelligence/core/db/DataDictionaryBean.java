package org.openbusinessintelligence.core.db;

import java.sql.*;
import javax.xml.parsers.*;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

/**
 * Class for replication of database tables between databases
 * @author Nicola Marangoni
 */
public class DataDictionaryBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataDictionaryBean.class);

    // Declarations of bean properties
	// Source properties
    private ConnectionBean sourceCon = null;
    private String sourceTable = "";
    private String sourceQuery = "";

    // Target properties
    private ConnectionBean targetCon = null;
    
    // Mapping properties
    private String mappingDefFile = "";
    private String[] sourceMapColumns = null;
    private String[] targetMapColumns = null;
    private String[] targetDefaultColumns = null;
    private String[] targetDefaultValues = null;

    // Declarations of internally used variables
    private String[] sourceColumnNames = null;
    private String[] sourceColumnType = null;
    private int[] sourceColumnLength = null;
    private int[] sourceColumnPrecision = null;
    private int[] sourceColumnScale = null;
    private String[] sourceColumnDefinition = null;
    //
    private String[] targetColumnNames = null;
    private String[] targetColumnType = null;
    private int[] targetColumnLength = null;
    private int[] targetColumnPrecision = null;
    private int[] targetColumnScale = null;
    private String[] targetColumnDefinition = null;
    
    private int[] columnPkPositions = null;
    private String[] targetColumnInPk = null;
    private String[] targetColumnNonInPk = null;
    
    // Constructor
    public DataDictionaryBean() {
        super();
    }

    // Set source properties methods
    public void setSourceTable(String property) {
        sourceTable = property;
    }

    public void setSourceQuery(String property) {
        sourceQuery = property;
    }
    
    public void setSourceConnection(ConnectionBean property) {
    	sourceCon = property;
    }
    
    public void setTargetConnection(ConnectionBean property) {
    	targetCon = property;
    }
    

    // Set optional mapping properties 
    public void setMappingDefFile(String property) {
    	mappingDefFile = property;
    }
    
    public void setSourceMapColumns(String[] property) {
    	sourceMapColumns = property;
    }
    
    public void setTargetMapColumns(String[] property) {
    	targetMapColumns = property;
    }
    
    public void setTargetDefaultColumns(String[] property) {
    	targetDefaultColumns = property;
    }
    
    
    // Get methods    
    public String[] getSourceColumnNames() {
    	return sourceColumnNames;
    }
    
    public String[] getSourceColumnDefinition() {
    	return sourceColumnDefinition;
    }
    
    public String[] getTargetColumnNames() {
    	return targetColumnNames;
    }
    
    public String[] getTargetColumnDefinition() {
    	return targetColumnDefinition;
    }
    
    public int[] getSourceColumnPkPositions() {
    	return columnPkPositions;
    }
    
    // Execution methods
    public void retrieveColumns() throws Exception {
    	
    	logger.debug("Getting columns for source...");

    	String sourceProductName;
    	String targetProductName;
    	String sourcePrefix = "";
    	if (!(sourceCon.getSchemaName() == null || sourceCon.getSchemaName().equals(""))) {
    		sourcePrefix = sourceCon.getSchemaName() + ".";
    		logger.debug("Prefix for source table: " + sourcePrefix);
    	}
    	
    	String sqlText;
       	if (sourceQuery == null || sourceQuery.equals("")) {
       		sqlText = "SELECT * FROM " + sourcePrefix + sourceTable;
       		logger.debug(sqlText);
       	}
       	else {
       		sqlText = sourceQuery;
       	}
    	
       	logger.info("SQL: " + sqlText + ": getting columns...");
        
       	//openSourceConnection();
       	sourceProductName = sourceCon.getDatabaseProductName();
        targetProductName = targetCon.getDatabaseProductName();
        PreparedStatement columnStmt = sourceCon.getConnection().prepareStatement(sqlText);
        ResultSet rs = columnStmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();

        sourceColumnNames = new String[rsmd.getColumnCount()];
        sourceColumnType = new String[rsmd.getColumnCount()];
        sourceColumnLength = new int[rsmd.getColumnCount()];
        sourceColumnPrecision = new int[rsmd.getColumnCount()];
        sourceColumnScale = new int[rsmd.getColumnCount()];
        sourceColumnDefinition = new String[rsmd.getColumnCount()];
        //
        targetColumnNames = new String[rsmd.getColumnCount()];
        targetColumnType = new String[rsmd.getColumnCount()];
        targetColumnLength = new int[rsmd.getColumnCount()];
        targetColumnPrecision = new int[rsmd.getColumnCount()];
        targetColumnScale = new int[rsmd.getColumnCount()];
        targetColumnDefinition = new String[rsmd.getColumnCount()];
        columnPkPositions = new int[rsmd.getColumnCount()];
        
        logger.info("Source RDBMS product: " + sourceProductName);
        logger.info("Target RDBMS product: " + targetProductName);
        
        TypeConversionBean typeConverter = new TypeConversionBean();
        
       	for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        	sourceColumnNames[i - 1] = rsmd.getColumnName(i).toUpperCase();
        	targetColumnNames[i - 1] = sourceColumnNames[i - 1];
            targetColumnNames[i - 1] = targetCon.getColumnIdentifier(targetColumnNames[i - 1]);
            
        	if (sourceMapColumns != null) {
	        	for (int mc = 0; mc < sourceMapColumns.length; mc++) {
	        		if (sourceMapColumns[mc].equalsIgnoreCase(sourceColumnNames[i - 1])) {
	        			sourceColumnNames[i - 1] = targetMapColumns[mc];
	        			targetColumnNames[i - 1] = targetMapColumns[mc];
	        		}
	        	}
        	}

        	//*******************************
        	// set source column properties
        	sourceColumnType[i - 1] = rsmd.getColumnTypeName(i).toUpperCase();
        	sourceColumnLength[i - 1] = rsmd.getColumnDisplaySize(i);
        	sourceColumnPrecision[i - 1] = rsmd.getPrecision(i);
        	sourceColumnScale[i - 1] = rsmd.getScale(i);
        	sourceColumnDefinition[i - 1] = sourceColumnType[i - 1];
        	
        	//******************************
        	// Use type converter
        	typeConverter.setSourceProductName(sourceProductName);
        	typeConverter.setTargetProductName(targetProductName);
        	typeConverter.setSourceColumnType(sourceColumnType[i - 1]);
        	typeConverter.setSourceColumnLength(sourceColumnLength[i - 1]);
        	typeConverter.setSourceColumnPrecision(sourceColumnPrecision[i - 1]);
        	typeConverter.setSourceColumnScale(sourceColumnScale[i - 1]);
        	
        	typeConverter.convert();

        	sourceColumnDefinition[i - 1] = typeConverter.getSourceColumnDefinition();
        	
        	targetColumnType[i - 1] = typeConverter.getTargetColumnType();
        	targetColumnLength[i - 1] = typeConverter.getTargetColumnLength();
        	targetColumnPrecision[i - 1] = typeConverter.getTargetColumnPrecision();
        	targetColumnScale[i - 1] = typeConverter.getTargetColumnScale();
        	targetColumnDefinition[i - 1] = typeConverter.getTargetColumnDefinition();
        	
        	logger.debug("Target column " + (i) + "  Name: " + sourceColumnNames[i - 1] + " Type: " + targetColumnType[i - 1] + "  Length: " + targetColumnLength[i - 1] + " Precision: " + targetColumnPrecision[i - 1] + " Scale: " +targetColumnScale[i - 1]);       	
           	logger.debug("Target column " + (i) + "  Name: " + sourceColumnNames[i - 1] + "  Definition: " + targetColumnDefinition[i - 1]);
       	}
        rs.close();
        columnStmt.close();

        logger.info("got column properties");

        try {
            String schema = null;
            if (sourceTable.split("\\.").length==2) {
                schema = sourceTable.split("\\.")[0];
                logger.info("Schema: " + schema);
            }

            // Get information about primary keys
            logger.info("get primary key information...");
            logger.debug("Table: " + sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);

            ResultSet rspk = sourceCon.getConnection().getMetaData().getPrimaryKeys(schema, schema, sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);
            int pkLength = 0;
            while (rspk.next()) {
                logger.info("PRIMARY KEY Position: " + rspk.getObject("KEY_SEQ") + " Column: " + rspk.getObject("COLUMN_NAME"));
                for (int i = 0; i < sourceColumnNames.length; i++) {
                    if (sourceColumnNames[i].equalsIgnoreCase(rspk.getString("COLUMN_NAME"))) {
                        columnPkPositions[i] = rspk.getInt("KEY_SEQ");
                        pkLength++;
                    }
                }
            }
            rspk.close();
            logger.info("got primary key information...");

            if (pkLength>0) {
                targetColumnInPk = new String[pkLength];
                targetColumnNonInPk = new String[targetColumnNames.length - pkLength];
                int iPk = 0;
                int nPk = 0;
                for (int i = 0; i < targetColumnNames.length; i++) {
                    if (columnPkPositions[i]>=1) {
                        targetColumnInPk[iPk] = targetColumnNames[i];
                        iPk++;
                    }
                    else {
                    	targetColumnNonInPk[nPk] = targetColumnNames[i];
                        nPk++;
                    }
                }
            }
            else {
                targetColumnNonInPk = targetColumnNames;
            }

        }
        catch (Exception e) {
            logger.error(e.toString());
            throw e;
        }

        logger.info("got primary key information");
    }
    
    public void retrieveMappingDefinition() throws Exception {
    	
    	// Load mapping definition file
    	logger.info("LOADING MAP DEFINITION FILE " + mappingDefFile + "...");
    	
    	org.w3c.dom.Document mappingXML = null;
    	
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
}