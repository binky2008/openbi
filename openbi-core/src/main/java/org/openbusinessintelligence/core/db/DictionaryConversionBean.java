package org.openbusinessintelligence.core.db;

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
    private String sourceSchema = "";
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
    private int[] columnPkPosition = null;
    private int[] columnJdbcType = null;
    //
    private String[] sourceColumnNames = null;
    private String[] sourceColumnType = null;
    private String[] sourceColumnTypeAttribute = null;
    private int[] sourceColumnLength = null;
    private int[] sourceColumnPrecision = null;
    private int[] sourceColumnScale = null;
    private String[] sourceColumnDefinition = null;
    //
    private String[] targetColumnNames = null;
    private String[] targetColumnType = null;
    private String[] targetColumnTypeAttribute = null;
    private int[] targetColumnLength = null;
    private int[] targetColumnPrecision = null;
    private int[] targetColumnScale = null;
    private String[] targetColumnDefinition = null;
    
    // Constructor
    public DictionaryConversionBean() {
        super();
    }

    // Set source properties methods
    public void setSourceSchema(String property) {
        sourceSchema = property;
    }
    
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
    
    public int[] getColumnJdbcType() {
    	return columnJdbcType;
    }
    
    public int[] getSourceColumnPkPositions() {
    	return columnPkPosition;
    }
    
    public void retrieveColumns() throws Exception {
    	
    	TableDictionaryBean sourceDictionary = new TableDictionaryBean();
    	sourceDictionary.setSourceConnection(sourceCon);
    	sourceDictionary.setSourceSchema(sourceSchema);
    	sourceDictionary.setSourceTable(sourceTable);
    	sourceDictionary.setSourceQuery(sourceQuery);
    	//
    	sourceDictionary.retrieveColumns();
    	//
    	columnCount = sourceDictionary.getColumnCount();
    	sourceColumnNames = sourceDictionary.getColumnNames();
        sourceColumnType = sourceDictionary.getColumnTypes();
        sourceColumnTypeAttribute = sourceDictionary.getColumnTypeAttribute();
        sourceColumnLength = sourceDictionary.getColumnLength();
        sourceColumnPrecision = sourceDictionary.getColumnPrecision();
        sourceColumnScale = sourceDictionary.getColumnScale();
        columnJdbcType = sourceDictionary.getColumnJdbcType();
        
    	String sourceProductName;
    	String targetProductName;
       	sourceProductName = sourceCon.getDatabaseProductName();
        targetProductName = targetCon.getDatabaseProductName();
        
        logger.info("Source RDBMS product: " + sourceProductName);
        logger.info("Target RDBMS product: " + targetProductName);
        
        sourceColumnDefinition = new String[columnCount];
        
        targetColumnNames = new String[columnCount];
        targetColumnType = new String[columnCount];
        targetColumnTypeAttribute = new String[columnCount];
        targetColumnLength = new int[columnCount];
        targetColumnPrecision = new int[columnCount];
        targetColumnScale = new int[columnCount];
        targetColumnDefinition = new String[columnCount];
        columnPkPosition = new int[columnCount];
		
    	// Load type convertion matrix
		org.w3c.dom.Document convertionMatrix = null;

		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			convertionMatrix = docBuilder.parse(Thread.currentThread().getContextClassLoader().getResource("datatypes/convertionMatrix.xml").toString());
			convertionMatrix.getDocumentElement().normalize();
		}
		catch(Exception e) {
			logger.error("Cannot load option file: " + e.getMessage());
			e.printStackTrace();
		    throw e;
		}
        
        TypeConversionBean typeConverter = new TypeConversionBean();
        typeConverter.setConvertionMatrix(convertionMatrix);
        
       	for (int i = 0; i < columnCount; i++) {
        	//sourceColumnNames[i] = rsmd.getColumnName(i).toUpperCase();
        	targetColumnNames[i] = sourceColumnNames[i];
            targetColumnNames[i] = targetCon.getColumnIdentifier(targetColumnNames[i]);
            
            logger.info("get mapped column...");
        	if (sourceMapColumns != null) {
	        	for (int mc = 0; mc < sourceMapColumns.length; mc++) {
	        		if (sourceMapColumns[mc].equalsIgnoreCase(sourceColumnNames[i])) {
	        			sourceColumnNames[i] = targetMapColumns[mc];
	        			targetColumnNames[i] = targetMapColumns[mc];
	        		}
	        	}
        	}
            logger.info("got mapped column");
        	
        	//******************************
        	// Use type converter
        	typeConverter.setSourceProductName(sourceProductName);
        	typeConverter.setTargetProductName(targetProductName);
        	typeConverter.setSourceColumnType(sourceColumnType[i]);
        	typeConverter.setSourceColumnTypeAttribute(sourceColumnTypeAttribute[i]);
        	typeConverter.setSourceColumnLength(sourceColumnLength[i]);
        	typeConverter.setSourceColumnPrecision(sourceColumnPrecision[i]);
        	typeConverter.setSourceColumnScale(sourceColumnScale[i]);

            logger.debug("convert datatype...");
        	typeConverter.convert();
            logger.debug("datatype converted");
            
            sourceColumnDefinition[i] = typeConverter.getSourceColumnDefinition();
        	
        	targetColumnType[i] = typeConverter.getTargetColumnType();
        	targetColumnTypeAttribute[i] = typeConverter.getTargetColumnTypeAttribute();
        	targetColumnLength[i] = typeConverter.getTargetColumnLength();
        	targetColumnPrecision[i] = typeConverter.getTargetColumnPrecision();
        	targetColumnScale[i] = typeConverter.getTargetColumnScale();
        	targetColumnDefinition[i] = typeConverter.getTargetColumnDefinition();
        	
        	logger.debug("Source column " + (i) + "  Name: " + sourceColumnNames[i] + " Type: " + sourceColumnType[i] + " Attribute: " + sourceColumnTypeAttribute[i] + "  Length: " + sourceColumnLength[i] + " Precision: " + sourceColumnPrecision[i] + " Scale: " +sourceColumnScale[i] + "  Definition: " + sourceColumnDefinition[i]);       	
        	logger.debug("Target column " + (i) + "  Name: " + targetColumnNames[i] + " Type: " + targetColumnType[i] + " Attribute: " + targetColumnTypeAttribute[i] + "  Length: " + targetColumnLength[i] + " Precision: " + targetColumnPrecision[i] + " Scale: " +targetColumnScale[i] + "  Definition: " + targetColumnDefinition[i]);       	
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