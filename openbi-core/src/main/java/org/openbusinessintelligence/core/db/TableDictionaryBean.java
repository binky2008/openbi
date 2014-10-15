package org.openbusinessintelligence.core.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
    private String sourceSchema = "";
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
    private String[] columnTypeAttribute = null;
    private int[] columnLength = null;
    private int[] columnPrecision = null;
    private int[] columnScale = null;
    private String[] columnDefinition = null;
    private int[] columnJdbcType = null;
    
    private ResultSet columnRS;
    
    // Constructor
    public TableDictionaryBean() {
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
    
    public String[] getColumnTypeAttribute() {
    	return columnTypeAttribute;
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
    
    public int[] getColumnJdbcType() {
    	return columnJdbcType;
    }
    
    public String[] getColumnDefinition() {
    	return columnDefinition;
    }
    
    public int[] getColumnPkPositions() {
    	return columnPkPositions;
    }
    
    // Execution methods
    public void retrieveColumns() throws Exception {
    	
    	logger.debug("Getting columns for source...");

    	String productName = sourceCon.getDatabaseProductName();
    	String defaultSchema = sourceCon.getSchemaName();
        logger.info("Source RDBMS product: " + productName);
        logger.info("Default schema: " + defaultSchema);
        logger.info("Table schema: " + sourceSchema);

    	String sourcePrefix = "";
    	if (!(sourceSchema == null || sourceSchema.equals(""))) {
    		sourcePrefix = sourceSchema + ".";
    		sourceSchema = sourceSchema.toUpperCase();
    	}
		logger.debug("Prefix for source table: " + sourcePrefix);
    	
        List<String> listName = new ArrayList<String>();
        List<String> listType = new ArrayList<String>();
        List<Integer> listLength = new ArrayList<Integer>();
        List<Integer> listPrecision = new ArrayList<Integer>();
        List<Integer> listScale = new ArrayList<Integer>();
        List<Integer> listJdbcType = new ArrayList<Integer>();
        
        if (
        	(
        		!(sourceTable == null || sourceTable.equals(""))
        	) &&
        	(
        		productName.toUpperCase().contains("IMPALA") ||
        		productName.toUpperCase().contains("POSTGRES") ||
        		productName.toUpperCase().contains("DB2")
        	)
        ) {
        	sourceQuery = "SELECT * FROM " + sourcePrefix + sourceTable;
        }
        else if (
        	productName.toUpperCase().contains("NETEZZA") &&
        	(sourceSchema == null || sourceSchema.equals(""))
        ) {
        	sourceQuery = "SELECT * FROM " + sourcePrefix + sourceTable;
        }
        
       	if (sourceQuery == null || sourceQuery.equals("")) {
            DatabaseMetaData dbmd = sourceCon.getConnection().getMetaData();
            if (productName.toUpperCase().contains("VECTOR")) {
                columnRS = dbmd.getColumns(null, sourceSchema, sourceTable.toLowerCase(), null);
            }
            else if (!productName.toUpperCase().contains("MYSQL")) {
                columnRS = dbmd.getColumns(null, sourceSchema, sourceTable.toUpperCase(), null);
            }
            else {
                columnRS = dbmd.getColumns(null, sourceSchema, sourceTable, null);
            }
    	    while (columnRS.next()) {
    	    	if (columnRS.getString("TABLE_NAME").equalsIgnoreCase(sourceTable)) {
        	    	listName.add(columnRS.getString("COLUMN_NAME"));
        	    	listType.add(columnRS.getString("TYPE_NAME"));
            	    listLength.add(columnRS.getInt("COLUMN_SIZE"));
        	    	listPrecision.add(columnRS.getInt("COLUMN_SIZE"));
    	    		listScale.add(columnRS.getInt("DECIMAL_DIGITS"));
    	    		listJdbcType.add(columnRS.getInt("DATA_TYPE"));
    	    	}
    	    }
    	    columnRS.close();
    		
            columnCount = listName.size();
            
       	}
       	else {
       		PreparedStatement columnStmt = null;
       		ResultSet rs = null;
	    	
       		logger.debug("Source query: " + sourceQuery);
	        columnStmt = sourceCon.getConnection().prepareStatement(sourceQuery);
	           
	        rs = columnStmt.executeQuery();
	        ResultSetMetaData rsmd = rs.getMetaData();
	         
	        columnCount = rsmd.getColumnCount();
	           
	        for (int i = 1; i <= columnCount; i++) {
	         	listName.add(rsmd.getColumnName(i).toUpperCase());
	         	listType.add(rsmd.getColumnTypeName(i).toUpperCase());
    	    	if (
    	    		productName.toUpperCase().contains("IMPALA") &&
    	    		rsmd.getColumnTypeName(i).equalsIgnoreCase("DECIMAL")
    	    	) {
    	    		listLength.add(38);
    	         	listPrecision.add(38);
    	           	listScale.add(38);
    	    	}
    	    	else {
    	         	listLength.add(rsmd.getColumnDisplaySize(i));
    	         	listPrecision.add(rsmd.getPrecision(i));
    	           	listScale.add(rsmd.getScale(i));
    	    	}
	           	listJdbcType.add(rsmd.getColumnType(i));
	        }
	        rs.close();
	        columnStmt.close();
       	}
       	
        columnNames = new String[columnCount];
        columnType = new String[columnCount];
        columnTypeAttribute = new String[columnCount];
        columnLength = new int[columnCount];
        columnPrecision = new int[columnCount];
        columnScale = new int[columnCount];
        columnDefinition = new String[columnCount];
        columnJdbcType = new int[columnCount];

        listName.toArray(columnNames);
        listType.toArray(columnType);
        
       	for (int i = 0; i < columnCount; i++) {
        	columnLength[i] = listLength.get(i);
        	columnPrecision[i] = listPrecision.get(i);
        	columnScale[i] = listScale.get(i);
        	columnJdbcType[i] = listJdbcType.get(i);
        	
        	columnTypeAttribute[i] = "";
        	columnDefinition[i] = columnType[i];
        	
        	// Search for type attribute(s)
        	if (
                productName.toUpperCase().contains("ORACLE") &&
                columnType[i].toUpperCase().contains("RAW")
            ) {
        		// Leave it as it is
            }
        	else if (
                productName.toUpperCase().contains("HSQL") &&
                columnType[i].toUpperCase().contains("BIT")
            ) {
        		// Leave it as it is
            }
        	else if (
            	productName.toUpperCase().contains("ORACLE") &&
                (
                	columnType[i].toUpperCase().contains("INTERVAL") ||
                	columnType[i].toUpperCase().contains("TIMESTAMP")
                )
            ) {
        		while (columnType[i].indexOf("(")>0) {
        			columnType[i] = columnType[i].split("\\(",2)[0] + columnType[i].split("\\)",2)[1];
        		}
            }
        	else if (
        		productName.toUpperCase().contains("TERADATA") &&
            	columnType[i].toUpperCase().contains("PERIOD")
            ) {
        		logger.debug("Teradata PERIOD type");
            	if (columnType[i].contains("(")) {
            		logger.debug("Separating attribute...");
            		columnTypeAttribute[i] = columnType[i].substring(columnType[i].indexOf("("));
            		columnType[i] = columnType[i].split("\\(",2)[0];
            	}
            }
        	else if (
        		(
        			productName.toUpperCase().contains("DERBY") ||
        			productName.toUpperCase().contains("ANYWHERE") ||
        			productName.toUpperCase().contains("IQ") ||
        			productName.toUpperCase().contains("VERTICA")
        		) &&
        		columnType[i].toUpperCase().contains("LONG")
        	) {
        		if (columnType[i].split(" ").length > 2) {
            		columnTypeAttribute[i] = columnType[i].split(" ",3)[2];
            		columnType[i] = columnType[i].split(" ",3)[0] + " " + columnType[i].split(" ",3)[1];
        		}
        	}
        	else if (
            	productName.toUpperCase().contains("DERBY")&&
            	columnType[i].toUpperCase().contains("FOR BIT DATA")
            ) {
        		columnTypeAttribute[i] = "FOR BIT DATA";
        		columnType[i] = columnType[i].split(" ",2)[0];
            }
        	else if (
        		columnType[i].toUpperCase().contains("INTERVAL") ||
                columnType[i].toUpperCase().contains("TIMESTAMP") ||
                columnType[i].toUpperCase().contains("TIME")
            ) {
        		// Leave it as it is
            }
        	else if (columnType[i].split(" ").length > 1) {
        		columnTypeAttribute[i] = columnType[i].split(" ",2)[1];
        		columnType[i] = columnType[i].split(" ",2)[0];
        	}
        	else {
        		columnTypeAttribute[i] = "";
        	}
        	
        	// Source definition
        	if (columnScale[i] > 0) {
        		columnDefinition[i] += "(" + columnPrecision[i] + "," + columnScale[i] + ")";
        	}
        	else if (columnLength[i] > 0) {
        		columnDefinition[i] += "(" + columnLength[i] + ")";
        	}
        	columnDefinition[i] += " " + columnTypeAttribute[i];
        	
        	logger.debug(
        		"Column " + (i) +
        		" Name: " + columnNames[i] +
        		" Type: " + columnType[i] +
        		" Length: " + columnLength[i] +
        		" Precision: " + columnPrecision[i] +
        		" Scale: " +columnScale[i] + 
        		" Attribute: " + columnTypeAttribute[i] +
        		" JDBC Type:" + columnJdbcType[i]
        	);
       	}

        logger.info("got column properties");

        logger.info("getting pk properties");
        columnPkPositions = new int[columnCount];

        // Get information about primary keys
        try {
        	int pkLength = 0;
            if (
            	!(sourceTable.equalsIgnoreCase("")) &&
            	!(sourceTable == null)
            ) {
                String schema = null;
                if (sourceTable.split("\\.").length==2) {
                    schema = sourceTable.split("\\.")[0];
                    logger.info("Schema: " + schema);
                }

                logger.info("get primary key information...");
                logger.debug("Table: " + sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);

                ResultSet rspk = sourceCon.getConnection().getMetaData().getPrimaryKeys(schema, schema, sourceTable.split("\\.")[sourceTable.split("\\.").length-1]);
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
            }

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
            logger.info("got pk properties");

        }
        catch (Exception e) {
            logger.error(e.toString());
            throw e;
        }

        logger.info("got primary key information");
    }
}