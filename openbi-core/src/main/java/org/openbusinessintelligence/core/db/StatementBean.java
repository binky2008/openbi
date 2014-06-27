package org.openbusinessintelligence.core.db;

/**
 * @author marangon
 *
 */
public final class StatementBean {
	
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
