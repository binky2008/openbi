package org.openbusinessintelligence.core.db;

import java.io.FileInputStream;
import java.sql.*;
import java.io.*;
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.openbusinessintelligence.core.file.*;
import org.slf4j.LoggerFactory;

public class ConnectionBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ConnectionBean.class);
	
    // Declarations of bean properties
	//  properties
    private String propertyFile = "";
    private String databaseDriver = "";
    private String connectionURL = "";
    private String userName = "";
    private String passWord = "";
    private String dataSourceName = "";
    private String catalogName = "";
    private String schemaName = "";
    private String keyWords = "";
    private String keyWordFile = "";
    private String quoteString = "";
    private int maxRowSize = 0;
    //
    private Connection connection = null;
    private DatabaseMetaData metadata = null;
    private String databaseProductName = null;
    
    // Constructor
    public ConnectionBean() {
        super();
    }

    // Setter methods
    public void setPropertyFile(String property) {
    	propertyFile = property;
    }
    
    public void setDataSourceName(String property) {
    	dataSourceName = property;
    }

    public void setDatabaseDriver(String property) {
        databaseDriver = property;
    }

    public void setConnectionURL(String property) {
        connectionURL = property;
    }

    public void setUserName(String property) {
        userName = property;
    }

    public void setPassWord(String property) {
        passWord = property;
    }

    public void setCatalogName(String property) {
        catalogName = property;
    }

    public void setSchemaName(String property) {
        schemaName = property;
    }

    public void setKeyWordFile(String property) {
    	keyWordFile = property;
    }

    // Getter methods
    public Connection getConnection() {
    	return connection;
    }
    
    public String getCatalogName() {
    	return catalogName;
    }
    
    public String getSchemaName() {
    	return schemaName;
    }

    public String getDatabaseProductName() {
    	return databaseProductName;
    }

    public int getMaxRowSize() {
    	return maxRowSize;
    }
    
    // Get normalizer obejct name
    public String getNormalizedObjectName(String name, String prefix, String suffix) throws Exception {

    	String normalizedRoot = name;
    	String normalizedName = "";
    	int objectMaxLength;
    	int rootMaxLength;
    	int prefixLength;
    	int suffixLength;
    	
    	objectMaxLength = metadata.getMaxTableNameLength();
    			
		prefixLength = prefix.length();
		suffixLength = suffix.length();
		
    	rootMaxLength = objectMaxLength - prefixLength - suffixLength;
		
		logger.debug("Object max length: " + objectMaxLength);
		logger.debug("Prefix length: " + prefixLength);
		logger.debug("Suffix length " + suffixLength);
		logger.debug("Root max length: " + rootMaxLength);
    	
		if (name.length() > rootMaxLength) {
			logger.debug("Normalized root: " + name.substring(0, rootMaxLength - 1));
			normalizedRoot = name.substring(0, rootMaxLength - 1);
			logger.debug("Normalized name: " + normalizedName);
		}
		normalizedName = prefix + normalizedRoot + suffix;
    	
    	return normalizedName;
    }
    
    // Get complete identifier string
    public String getObjectIdentifier(String objectName) {
		logger.debug("Getting complete identifier for object " + objectName);
		
		String identifier = "";
		if (!(catalogName == null || catalogName.equals(""))) {
	    	if (keyWords.contains(catalogName.toUpperCase())) {
	    		identifier += quoteString + catalogName + quoteString + ".";
	    	}
	    	else {
	    		identifier += catalogName + ".";
	    	}
	    }
	    if (!(schemaName == null || schemaName.equals(""))) {
	    	if (keyWords.contains(schemaName.toUpperCase())) {
	    		identifier += quoteString + schemaName + quoteString + ".";
	    	}
	    	else {
	    		identifier += schemaName + ".";
	    	}
	    }
    	if (keyWords.contains(objectName.toUpperCase())) {
    		identifier += quoteString + objectName + quoteString;
    	}
    	else {
    		identifier += objectName;
    	}
    	
    	return identifier;
    }
    
    // Get column string
    public String getColumnIdentifier(String columnName) throws Exception {
		logger.debug("Getting identifier for column " + columnName);
		boolean isNumber = false;
		try {
			Integer.parseInt(columnName);
			isNumber = true;
		}
		catch (Exception e) {
			isNumber = false;
		}

		String identifier = "";
		try {
	    	if (keyWords.contains(columnName.toUpperCase()) || isNumber) {
	    		identifier += quoteString + columnName.toUpperCase() + quoteString;
	    	}
	    	else {
	    		identifier += columnName;
	    	}
		}
		catch (Exception e) {
			logger.error(e.toString());
			throw e;
		}
    	
    	return identifier;
    }
    
    //Get value expression
    private String getValueExpression(Object property) {
    	String expression = property.toString();
    	return expression;
    }
    
    public String[] getTableList() throws Exception {
    	ResultSet dbTables = null;

    	logger.info("########################################");
    	logger.debug("RDBMS type: " + databaseProductName);
    	logger.debug("Get tables for schema: " + schemaName);
    	try {
	    	if (databaseProductName.toUpperCase().contains("MYSQL")) {
	        	logger.debug("Serching in a jdbc catalog...");
		    	dbTables = metadata.getTables(schemaName, null, null, null);
		        logger.debug("Tables obtained");
	    	}
	    	else if (databaseProductName.toUpperCase().contains("ORACLE") || databaseProductName.toUpperCase().contains("DB2")) {
	        	logger.debug("Serching in a jdbc schema...");
	        	dbTables = metadata.getTables(null, schemaName.toUpperCase(), null, null);
		        logger.debug("Tables obtained");
	    	}
	    	else {
	    		dbTables = metadata.getTables(null, schemaName, null, null);
		        logger.debug("Tables obtained");
	    	}
    	}
		catch (Exception e) {
			logger.error("Exception: \n" + e.toString());
			throw e;
		}
    	String[] tableList = null;
    	try {
    		List<String> tableArray = new ArrayList<String>();
	    	while (dbTables.next()) {
	    		tableArray.add(dbTables.getString("TABLE_NAME"));
	    		logger.info(dbTables.getString("TABLE_SCHEM") + "." + dbTables.getString("TABLE_NAME"));
	    	}
	    	tableList = new String[tableArray.size()];
	    	int i = 0;
	    	for(String table : tableArray) {
	    		tableList[i] = table;
	    		i++;
	    	}
    	}
		catch (Exception e) {
			logger.error("Exception: \n" + e.toString());
			throw e;
		}    	
    	return tableList;
    }
    
    // Execution methods
    public void openConnection() throws Exception  {
    	
    	logger.info("Opening connection...");
	    if (dataSourceName == null ||dataSourceName.equals("")) {
	      	// Get connection using driver
	        if (propertyFile == null || propertyFile.equals("")) {
	          	// Use given username and password
	           	logger.info("Using username & password");
		       	Class.forName(databaseDriver).newInstance();
		       	logger.info("Loaded database driver " + databaseDriver);
	           	connection = DriverManager.getConnection(connectionURL, userName, passWord);
	        }
	       	else {
	           	// Use property file
	           	logger.info("Using property file " + propertyFile);
	           	Properties connectionProperties = new Properties();
	           	connectionProperties.load(new FileInputStream("datasources/" + propertyFile + ".properties"));
	       		if (databaseDriver == null || databaseDriver.equals("")) {
	       			// Get driver and url from property file
	       			databaseDriver = connectionProperties.getProperty("driver");
	       			logger.debug("databaseDriver = " + databaseDriver);
	       			connectionURL = connectionProperties.getProperty("url");
	       			logger.debug("connectionURL = " + connectionURL);
	       		}
		       	Class.forName(databaseDriver).newInstance();
		       	logger.debug("driver loaded");
	       		connection = DriverManager.getConnection(connectionURL, connectionProperties);
	       	}
	       	logger.debug("Connected to database " + connectionURL);
	    }
	    else {
	       	// Get connection from application server
	       	InitialContext ic = new InitialContext();
	       	DataSource ds = (DataSource)ic.lookup("java:comp/env/jdbc/" + dataSourceName.toLowerCase());
	       	connection = ds.getConnection();
	       	logger.debug("Connected to database " + dataSourceName);
	 	}
	       
	   	logger.info("Opened connection");
		metadata = connection.getMetaData();
	   	databaseProductName = metadata.getDatabaseProductName();
	   	logger.debug("Product: " + databaseProductName);
	   	
	   	InputStream keyWordStream;
	   	java.util.Scanner scanner;
	   	keyWords = "";
	   	
	   	// Get default SQL keywords
	   	keyWordStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("conf/SQL2003Keywords.txt");
	   	scanner = new java.util.Scanner(keyWordStream).useDelimiter("\\A");
	   	keyWords += scanner.next().replace("\n", ",");
	   	scanner.close();
	   	keyWordStream.close();
	   	
	   	// Load specific reserved keywords from a file
	   	if (keyWordFile != null && !(keyWordFile.equals(""))) {
	    	keyWordStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("conf/" + keyWordFile + ".txt");
	    	scanner = new java.util.Scanner(keyWordStream).useDelimiter("\\A");
	    	keyWords += scanner.next().replace("\n", ",");
	    	scanner.close();
	    	keyWordStream.close();
	   	}
    	keyWords = keyWords.replace("\r", "");
	   	
	   	// Get reserved keywords throw jdbc
	   	try {
		   	logger.debug("Getting keywords...");
		   	keyWords += metadata.getSQLKeywords();
		   	logger.debug("Keywords: " + keyWords);
	   	}
	   	catch (Exception e) {
	   		logger.error(e.getMessage());
	   	}
	   	try {
		   	logger.debug("Getting quote string...");
		   	quoteString = metadata.getIdentifierQuoteString();
		   	logger.debug("Quote string: " + quoteString);
	   	}
	   	catch (Exception e) {
	   		logger.error(e.getMessage());
	   	}
	   	try {
		   	logger.debug("Getting max row size...");
		   	maxRowSize = metadata.getMaxRowSize();
		   	logger.info("Max row size: " + maxRowSize);
	   	}
	   	catch (Exception e) {
	   		logger.error(e.getMessage());
	   	}
	   	
	   	// Get catalogues and schemas
		logger.info("########################################");
	   	logger.info("Found catalogs:");
	   	ResultSet dbCatalogs = metadata.getCatalogs();
	   	ResultSetMetaData rsmd = dbCatalogs.getMetaData();
	   	while (dbCatalogs.next()) {
	   		try {
		   		logger.info(dbCatalogs.getString("TABLE_CAT"));
	   		}
	   		catch (Exception e) {
		   		logger.info(dbCatalogs.getString(1));
	   		}
	   	}
		logger.info("########################################");
	   	logger.info("Found schemas:");
	   	ResultSet dbSchemas = metadata.getSchemas();
	   	while (dbSchemas.next()) {
	   		logger.info(dbSchemas.getString("TABLE_SCHEM"));
	   	}
	   	
	   	// Get types
		logger.info("########################################");
	   	logger.info("Found types:");
	   	ResultSet dbTypes = metadata.getTypeInfo();    	
	   	while (dbTypes.next()) {
	   		logger.info(dbTypes.getString("TYPE_NAME") + " " + dbTypes.getString("CREATE_PARAMS"));
	   	}
    }
    
    public void closeConnection() throws Exception {
    	logger.info("Closing connection");
    	connection.close();
    	logger.info("Closed connection");
    }

}
