package org.openbusinessintelligence.core.db;

import java.sql.*;

import javax.xml.parsers.*;

import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

/**
 * Class for replication of database tables between databases
 * @author Nicola Marangoni
 */
public class DictionaryConversionBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DictionaryConversionBean.class);

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
    private int columnCount = 0;
    private int[] columnPkPositions = null;
    private String[] targetColumnInPk = null;
    private String[] targetColumnNonInPk = null;
    //
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
    
    // Constructor
    public DictionaryConversionBean() {
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
    	
    	TableDictionaryBean sourceDictionary = new TableDictionaryBean();
    	sourceDictionary.setSourceConnection(sourceCon);
    	sourceDictionary.setSourceTable(sourceTable);
    	sourceDictionary.setSourceQuery(sourceQuery);
    	//
    	sourceDictionary.retrieveColumns();
    	//
    	columnCount = sourceDictionary.getColumnCount();
    	sourceColumnNames = sourceDictionary.getColumnNames();
        sourceColumnType = sourceDictionary.getColumnTypes();
        sourceColumnLength = sourceDictionary.getColumnLength();
        sourceColumnPrecision = sourceDictionary.getColumnPrecision();
        sourceColumnScale = sourceDictionary.getColumnScale();
        
    	String sourceProductName;
    	String targetProductName;
       	sourceProductName = sourceCon.getDatabaseProductName();
        targetProductName = targetCon.getDatabaseProductName();
        
        logger.info("Source RDBMS product: " + sourceProductName);
        logger.info("Target RDBMS product: " + targetProductName);
        
        sourceColumnDefinition = new String[columnCount];
        
        targetColumnNames = new String[columnCount];
        targetColumnType = new String[columnCount];
        targetColumnLength = new int[columnCount];
        targetColumnPrecision = new int[columnCount];
        targetColumnScale = new int[columnCount];
        targetColumnDefinition = new String[columnCount];
        columnPkPositions = new int[columnCount];
        
        TypeConversionBean typeConverter = new TypeConversionBean();
        
       	for (int i = 1; i <= columnCount; i++) {
        	//sourceColumnNames[i - 1] = rsmd.getColumnName(i).toUpperCase();
        	targetColumnNames[i - 1] = sourceColumnNames[i - 1];
            targetColumnNames[i - 1] = targetCon.getColumnIdentifier(targetColumnNames[i - 1]);
            
            logger.info("get mapped column...");
        	if (sourceMapColumns != null) {
	        	for (int mc = 0; mc < sourceMapColumns.length; mc++) {
	        		if (sourceMapColumns[mc].equalsIgnoreCase(sourceColumnNames[i - 1])) {
	        			sourceColumnNames[i - 1] = targetMapColumns[mc];
	        			targetColumnNames[i - 1] = targetMapColumns[mc];
	        		}
	        	}
        	}
            logger.info("got mapped column");
        	
        	//******************************
        	// Use type converter
        	typeConverter.setSourceProductName(sourceProductName);
        	typeConverter.setTargetProductName(targetProductName);
        	typeConverter.setSourceColumnType(sourceColumnType[i - 1]);
        	typeConverter.setSourceColumnLength(sourceColumnLength[i - 1]);
        	typeConverter.setSourceColumnPrecision(sourceColumnPrecision[i - 1]);
        	typeConverter.setSourceColumnScale(sourceColumnScale[i - 1]);

            logger.debug("convert datatype...");
        	typeConverter.convert();
            logger.debug("datatype converted");
            
            sourceColumnDefinition[i - 1] = typeConverter.getSourceColumnDefinition();
        	
        	targetColumnType[i - 1] = typeConverter.getTargetColumnType();
        	targetColumnLength[i - 1] = typeConverter.getTargetColumnLength();
        	targetColumnPrecision[i - 1] = typeConverter.getTargetColumnPrecision();
        	targetColumnScale[i - 1] = typeConverter.getTargetColumnScale();
        	targetColumnDefinition[i - 1] = typeConverter.getTargetColumnDefinition();
        	
        	logger.debug("Target column " + (i) + "  Name: " + targetColumnNames[i - 1] + " Type: " + targetColumnType[i - 1] + "  Length: " + targetColumnLength[i - 1] + " Precision: " + targetColumnPrecision[i - 1] + " Scale: " +targetColumnScale[i - 1]);       	
           	logger.debug("Target column " + (i) + "  Name: " + targetColumnNames[i - 1] + "  Definition: " + targetColumnDefinition[i - 1]);
       	}
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