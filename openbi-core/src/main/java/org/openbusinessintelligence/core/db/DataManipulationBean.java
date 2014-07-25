package org.openbusinessintelligence.core.db;

import java.sql.*;

import org.openbusinessintelligence.core.data.RandomDataGeneratorBean;
import org.slf4j.LoggerFactory;

public class DataManipulationBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DataManipulationBean.class);

	private String sourceProductName = "";
	private String sourceType = "";
	private String sourceTypeAttribute = "";
	
	private String targetProductName = "";
	private String targetType = "";
	private String targetTypeAttribute = "";
	
	PreparedStatement statement;
    
	int position;
	
    // Constructor
    public DataManipulationBean() {
        super();
    }
    
    // Set methods
    public void setSourceProductName(String property) {
    	sourceProductName = property;
    }
    
    public void setSourceType(String property) {
    	sourceType = property;
    }
    
    public void setSourceTypeAttribute(String property) {
    	sourceTypeAttribute = property;
    }
    
    public void setTargetProductName(String property) {
    	targetProductName = property;
    }
    
    public void setTargetType(String property) {
    	targetType = property;
    }
    
    public void setTargetTypeAttribute(String property) {
    	targetTypeAttribute = property;
    }
    
    public void setStatement(PreparedStatement property) {
    	statement = property;
    }
    
    public void setPosition(int property) {
    	position = property;
    }
    
    /**
     * Get type
     **/
    public int getSQLType() {
    	
    	int sqlType = 0;

		if (
			(
				targetProductName.contains("ANYWHERE") ||
				targetProductName.contains("DERBY")
			) &&
			targetType.contains("XML")
		) {
			sqlType = java.sql.Types.CLOB;
  		}
		else if (
			targetProductName.contains("MICROSOFT") ||
			targetProductName.contains("ANYWHERE") ||
			targetProductName.contains("DERBY")
	    ) {
	       	if (targetTypeAttribute.contains("BIT")) {
	       		sqlType = Types.BINARY;
	    	}
	       	else if (targetType.equals("UNIQUEIDENTIFIER")) {
	        	sqlType = Types.BINARY;
	      	}
	        else if (targetType.toUpperCase().contains("XML")) {
	        	sqlType = Types.CLOB;
	      	}
	        else if (targetType.toUpperCase().contains("BINARY")) {
	        	sqlType = Types.BINARY;
	      	}
	        else if (targetType.toUpperCase().contains("BLOB")) {
	        	sqlType = Types.BLOB;
	      	}
	      	else if (targetType.toUpperCase().contains("DOUBLE")) {
	      		sqlType = Types.DOUBLE;
	      	}
	        else if (targetType.toUpperCase().contains("CLOB")) {
	        	sqlType = Types.CLOB;
	      	}
	      	else if (targetType.toUpperCase().contains("FLOAT")) {
	      		sqlType = Types.FLOAT;
	      	}
	      	else if (targetType.toUpperCase().contains("REAL")) {
	      		sqlType = Types.REAL;
	      	}
	      	else if (targetType.toUpperCase().contains("TEXT")) {
	      		sqlType = Types.CHAR;
	      	}
	        else if (targetType.toUpperCase().contains("TIMESTAMP")) {
	        	sqlType = Types.TIMESTAMP;
	      	}
	      	else if (targetType.toUpperCase().contains("DATE")) {
	      		sqlType = Types.DATE;
	      	}
	      	else if (targetType.toUpperCase().contains("TIME")) {
	      		sqlType = Types.TIME;
	      	}
	  		else if (targetType.toUpperCase().contains("CHAR")) {
	  			sqlType = Types.CHAR;
	  		}
	  		else if (targetType.toUpperCase().contains("VARCHAR")) {
	  			sqlType = Types.VARCHAR;
	  		}
	  		else if (targetType.toUpperCase().contains("NUMERIC")) {
	  			sqlType = Types.NUMERIC;
	  		}
	        else {
	           	sqlType = Types.NULL;
	        }
	    }
	    else {
	        sqlType = Types.NULL;
	    }
    	return sqlType;
    }
    
    
    /**
     * Set null to a statement
     **/
    public void setNull() throws Exception {
		if (
			targetProductName.contains("MICROSOFT") ||
			targetProductName.contains("ANYWHERE") ||
			targetProductName.contains("DERBY")
      	) {
        	if (targetTypeAttribute.contains("BIT")) {
        		statement.setNull(position, Types.BINARY);
      		}
        	else if (targetType.equals("UNIQUEIDENTIFIER")) {
        		statement.setNull(position, Types.BINARY);
      		}
        	else if (targetType.toUpperCase().contains("BLOB")) {
        		statement.setNull(position, Types.BLOB);
      		}
      		else if (targetType.toUpperCase().contains("FLOAT")) {
      			statement.setNull(position, Types.FLOAT);
      		}
      		else if (targetType.toUpperCase().contains("REAL")) {
      			statement.setNull(position, Types.REAL);
      		}
      		else if (targetType.toUpperCase().contains("TEXT")) {
      			statement.setNull(position, Types.CHAR);
      		}
          	else if (targetType.toUpperCase().contains("TIMESTAMP")) {
          		statement.setNull(position, Types.TIMESTAMP);
      		}
      		else if (targetType.toUpperCase().contains("DATE")) {
      			statement.setNull(position, Types.DATE);
      		}
      		else if (targetType.toUpperCase().contains("TIME")) {
      			statement.setNull(position, Types.TIME);
      		}
            else {
            	statement.setNull(position, Types.NULL);
            }
      	}
        else {
        	statement.setNull(position, Types.NULL);
        }
    }
}
