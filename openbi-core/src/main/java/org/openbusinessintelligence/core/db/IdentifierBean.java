package org.openbusinessintelligence.core.db;

import org.slf4j.LoggerFactory;

public class IdentifierBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataCopyBean.class);

    // Declarations of bean properties
    private ConnectionBean connection = null;
    private String objectName = "";
    
    // Constructor
    public IdentifierBean() {
        super();
    }

    // Set source properties methods
    public void setConnection(ConnectionBean property) {
    	connection = property;
    }

    public void setObjectName(String property) {
    	objectName = property;
    }
    
    // Get complete identifier string
    public String getIdentifier() {
		logger.debug("Getting complete identifier for object " + objectName);
		
		String identifier = "";
		
		try {
	    	String keyWords = connection.getConnection().getMetaData().getSQLKeywords();
	    	String quoteString = connection.getConnection().getMetaData().getIdentifierQuoteString();
	    	logger.debug("key words: " + keyWords);
	    	logger.debug("quote string" + quoteString);
	    	if (!(connection.getCatalogName() == null || connection.getCatalogName().equals(""))) {
	    		identifier = connection.getCatalogName() + ".";
	    	}
	    	if (!(connection.getSchemaName() == null || connection.getSchemaName().equals(""))) {
	    		identifier += connection.getSchemaName() + ".";
	    	}
	    	
	    	identifier += objectName;
		}
		catch (Exception e) {
			logger.error(e.getMessage());
		}
    	
    	return identifier;
    }
}
