package org.openbusinessintelligence.core.db;

import java.sql.*;
import org.slf4j.LoggerFactory;

/**
 * Class for replication of database tables between databases
 * @author Nicola Marangoni
 */
public class TableDictionaryBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TableDictionaryBean.class);

    // Declarations of bean properties
	// Source properties
    private ConnectionBean sourceCon = null;
    private String sourceTable = "";
    private String sourceQuery = "";

    // Declarations of internally used variables
    private int columnCount = 0;
    private int[] columnPkPositions = null;
    private String[] columnInPk = null;
    private String[] columnNonInPk = null;
    //
    private String[] columnNames = null;
    private String[] columnType = null;
    private int[] columnLength = null;
    private int[] columnPrecision = null;
    private int[] columnScale = null;
    
    // Constructor
    public TableDictionaryBean() {
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
    
    
    // Get methods
    public int getColumnCount() {
    	return columnCount;
    }
    
    public String[] getColumnInPk() {
    	return columnInPk;
    }
    
    public String[] getColumnNonInPk() {
    	return columnNonInPk;
    }
    
    public String[] getColumnNames() {
    	return columnNames;
    }
    
    public String[] getColumnTypes() {
    	return columnType;
    }
    
    public int[] getColumnLength() {
    	return columnLength;
    }
    
    public int[] getColumnPrecision() {
    	return columnPrecision;
    }
    
    public int[] getColumnScale() {
    	return columnScale;
    }
    
    public int[] getColumnPkPositions() {
    	return columnPkPositions;
    }
    
    // Execution methods
    public void retrieveColumns() throws Exception {
    	
    	logger.debug("Getting columns for source...");

    	String sourceProductName;
    	String sourcePrefix = "";
    	if (!(sourceCon.getSchemaName() == null || sourceCon.getSchemaName().equals(""))) {
    		sourcePrefix = sourceCon.getSchemaName() + ".";
    		logger.debug("Prefix for source table: " + sourcePrefix);
    	}
    	
    	String sqlText;
       	if (sourceQuery == null || sourceQuery.equals("")) {
       		sqlText = "SELECT * FROM " + sourcePrefix + sourceTable;
       	}
       	else {
       		sqlText = sourceQuery;
       	}
    	
       	logger.info("SQL: " + sqlText + ": getting columns...");
        
       	//openSourceConnection();
       	sourceProductName = sourceCon.getDatabaseProductName();
        logger.info("Source RDBMS product: " + sourceProductName);
        PreparedStatement columnStmt = sourceCon.getConnection().prepareStatement(sqlText);
        ResultSet rs = columnStmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        
        columnCount = rsmd.getColumnCount();
        columnNames = new String[columnCount];
        columnType = new String[columnCount];
        columnLength = new int[columnCount];
        columnPrecision = new int[columnCount];
        columnScale = new int[columnCount];
        //
        columnPkPositions = new int[columnCount];
                
       	for (int i = 1; i <= columnCount; i++) {
        	columnNames[i - 1] = rsmd.getColumnName(i).toUpperCase();

        	//*******************************
        	// set source column properties
        	columnType[i - 1] = rsmd.getColumnTypeName(i).toUpperCase();
        	columnLength[i - 1] = rsmd.getColumnDisplaySize(i);
        	columnPrecision[i - 1] = rsmd.getPrecision(i);
        	columnScale[i - 1] = rsmd.getScale(i);
        	
        	logger.debug("Column " + (i) + "  Name: " + columnNames[i - 1] + " Type: " + columnType[i - 1] + "  Length: " + columnLength[i - 1] + " Precision: " + columnPrecision[i - 1] + " Scale: " +columnScale[i - 1]);       	
       	}
        rs.close();
        columnStmt.close();

        logger.info("got column properties");

        // Get information about primary keys
        try {
            String schema = null;
            if (sourceTable.split("\\.").length==2) {
                schema = sourceTable.split("\\.")[0];
                logger.info("Schema: " + schema);
            }

            logger.info("get primary key information...");
            logger.debug("Table: " + sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);

            ResultSet rspk = sourceCon.getConnection().getMetaData().getPrimaryKeys(schema, schema, sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);
            int pkLength = 0;
            while (rspk.next()) {
                logger.info("PRIMARY KEY Position: " + rspk.getObject("KEY_SEQ") + " Column: " + rspk.getObject("COLUMN_NAME"));
                for (int i = 0; i < columnNames.length; i++) {
                    if (columnNames[i].equalsIgnoreCase(rspk.getString("COLUMN_NAME"))) {
                        columnPkPositions[i] = rspk.getInt("KEY_SEQ");
                        pkLength++;
                    }
                }
            }
            rspk.close();

            if (pkLength>0) {
                columnInPk = new String[pkLength];
                columnNonInPk = new String[columnNames.length - pkLength];
                int iPk = 0;
                int nPk = 0;
                for (int i = 0; i < columnNames.length; i++) {
                    if (columnPkPositions[i]>=1) {
                        columnInPk[iPk] = columnNames[i];
                        iPk++;
                    }
                    else {
                    	columnNonInPk[nPk] = columnNames[i];
                        nPk++;
                    }
                }
            }
            else {
                columnNonInPk = columnNames;
            }

        }
        catch (Exception e) {
            logger.error(e.toString());
            throw e;
        }

        logger.info("got primary key information");
    }
}