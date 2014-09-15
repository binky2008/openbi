package org.openbusinessintelligence.core.db;

import org.slf4j.LoggerFactory;

/**
 * @author marangon
 *
 */
public final class StatementBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(StatementBean.class);
	
	// Connection properties
    //private ConnectionBean connection = null;
    private String productName = "";
    
    // Source properties
    private String sourceSchema = "";
    private String sourceTable = "";
    
    // Target properties
    private String targetSchema = "";
    private String targetTable = "";
    
    // Constructor
    public StatementBean() {
        super();
    }
    
    // Set methods
    public void setProductName(String property) {
    	productName = property;
    }
    
    public void setSourceSchema(String property) {
    	sourceSchema = property;
    }
    
    public void setSourceTable(String property) {
    	sourceTable = property;
    }
    
    public void setTargetSchema(String property) {
    	targetSchema = property;
    }
    
    public void setTargetTable(String property) {
    	targetTable = property;
    }
    
    /**
     * Get if column type is unusable
     **/
    public boolean getColumnUsable (String dataType) {
       	if (
           	(dataType.toUpperCase().contains("SDO")) ||
       		(dataType.toUpperCase().contains("INTERVAL")) ||
       		(dataType.toUpperCase().contains("SERIAL")) ||
       		(dataType.toUpperCase().contains("POINT")) ||
       		(dataType.toUpperCase().contains("FILE")) ||
       		(
       			productName.toUpperCase().contains("SQL SERVER") &&
       			dataType.toUpperCase().contains("TIMESTAMP")
       		) ||
       		(
               	productName.toUpperCase().contains("EXASOL") &&
               	dataType.toUpperCase().contains("GEOMETRY")
            ) ||
       		(
                productName.toUpperCase().contains("IMPALA") &&
                dataType.toUpperCase().contains("DECIMAL")
            ) ||
       		(
           		productName.toUpperCase().contains("ORACLE") &&
           		dataType.toUpperCase().contains("LONG")
           	) ||
	    	(
	    		productName.toUpperCase().contains("DERBY") &&
			    (
			    	dataType.toUpperCase().contains("LOB") ||
			    	dataType.toUpperCase().contains("XML") ||
			    	dataType.toUpperCase().contains("LONG")
    			)
    		)
        ) {
        	return false;
       	}
       	else {
        	return true;
       	}
    }
    
    // Get statement methods
    public String getEmptyTable() {
    	
        String schemaPrefix = "";
        String emptyText = "";
        
        if (!productName.toUpperCase().contains("IMPALA")) {
            if (!(targetSchema == null || targetSchema.equals(""))) {
            	schemaPrefix = targetSchema + ".";
            }
            if (productName.toUpperCase().contains("TERADATA")) {
            	emptyText = "DELETE " + schemaPrefix + targetTable + " ALL";
            }
            else if (productName.toUpperCase().contains("FIREBIRD")) {
            	emptyText = "DELETE FROM " + schemaPrefix + targetTable;
            }
            else {
            	emptyText = "TRUNCATE TABLE " + schemaPrefix + targetTable;
	           	if (productName.toUpperCase().contains("DB2")) {
	           		emptyText += " IMMEDIATE";
	           	}
            }
        }
    	
    	return emptyText;
    }
	
}
