package org.openbusinessintelligence.core.db;

public class TypeConversionBean {

    private String sourceColumnType;
    private int sourceColumnLength;
    private int sourceColumnPrecision;
    private int sourceColumnScale;
    private String sourceColumnDefinition;
    //
    private String targetProductName;
    private String targetColumnType;
    private int targetColumnLength;
    private int targetColumnPrecision;
    private int targetColumnScale;
    private String targetColumnDefinition;
    
    // Setter methods
    public void setSourceColumnType(String property) {
    	sourceColumnType = property;
    }
    public void setSourceColumnLength(int property) {
    	sourceColumnLength = property;
    }
    public void setSourceColumnPrecision(int property) {
    	sourceColumnPrecision = property;
    }
    public void setSourceColumnScale(int property) {
    	sourceColumnScale = property;
    }
    public void setTargetProductName(String property) {
    	targetProductName = property;
    }
    
    // Getter methods
    public String getSourceColumnDefinition() {
    	return sourceColumnDefinition;
    }
    public String getTargetColumnType() {
    	return sourceColumnType;
    }
    public int getTargetColumnLength() {
    	return sourceColumnLength;
    }
    public int getTargetColumnPrecision() {
    	return sourceColumnPrecision;
    }
    public int getTargetColumnScale() {
    	return sourceColumnScale;
    }
    public String getTargetColumnDefinition() {
    	return targetColumnDefinition;
    }
    
    // Conversion method
    public void convert() {
    	// Source definition
    	if (sourceColumnPrecision > 0) {
    		sourceColumnDefinition += "(" + sourceColumnPrecision + "," + sourceColumnScale + ")";
    	}
    	else if (sourceColumnLength > 0) {
    		sourceColumnDefinition += "(" + sourceColumnLength + ")";
    	}
    	

       	//*******************************
    	// set target column properties
        // NCHAR and NVARCHAR types
        if (sourceColumnType.contains("NCHAR") && sourceColumnLength == 1) {
            targetColumnType = "NCHAR (1)";
        }
        else if (
            (
                (sourceColumnType.contains("NCHAR") || sourceColumnType.contains("NVARCHAR")) &&
                sourceColumnLength > 1
            ) ||
            (sourceColumnType.contains("UNIQUE"))
        ) {
            if (targetProductName.toUpperCase().contains("ORACLE")) {
                if (sourceColumnLength > 4000) {
                    targetColumnType = "CLOB";
                }
                else {
                    targetColumnType = "NVARCHAR2";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("DB2")) {
                if (sourceColumnLength > 32672) {
                    targetColumnType = "CLOB";
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("POSTGRES")) {
                if (sourceColumnLength > 10000000) {
                    targetColumnType = "TEXT";
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("MYSQL")) {
                if (sourceColumnLength > 255) {
                    targetColumnType = "LONGTEXT";
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("INFORMIX")) {
                if ((sourceColumnLength > 255) && (sourceColumnLength <= 32739)) {
                    targetColumnType = "LVARCHAR";
                }
                else if (sourceColumnLength > 32739) {
                	targetColumnType = "TEXT";
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("HDB")) {
                if (sourceColumnLength > 5000) {
                    targetColumnType = "CLOB";
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else {
                targetColumnType = "NVARCHAR";
                if (targetProductName.toUpperCase().contains("MICROSOFT") && (sourceColumnLength > 8000)) {
                    targetColumnLength = -1;
                }
                else {
                    targetColumnLength = sourceColumnLength;
                }
            }
        }
        // CHAR and VARCHAR types
    	else if (sourceColumnType.contains("CHAR") && sourceColumnLength == 1) {
       		targetColumnType = "CHAR (1)";
       	}
    	else if ((sourceColumnType.contains("CHAR") && sourceColumnLength > 1)) {
    		if (targetProductName.toUpperCase().contains("ORACLE")) {
        		if (sourceColumnLength > 4000) {
        			targetColumnType = "CLOB";
        		}
        		else {
        			targetColumnType = "VARCHAR2";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
    		else if (targetProductName.toUpperCase().contains("DB2")) {
        		if (sourceColumnLength > 32672) {
        			targetColumnType = "CLOB";
        		}
        		else {
        			targetColumnType = "VARCHAR";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
    		else if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		if (sourceColumnLength > 10000000) {
        			targetColumnType = "TEXT";
        		}
        		else {
        			targetColumnType = "VARCHAR";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
    		else if (targetProductName.toUpperCase().contains("MYSQL")) {
        		if (sourceColumnLength > 255) {
        			targetColumnType = "LONGTEXT";
        		}
        		else {
        			targetColumnType = "VARCHAR";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
            else if (targetProductName.toUpperCase().contains("INFORMIX")) {
                if ((sourceColumnLength > 255) && (sourceColumnLength <= 32739)) {
                    targetColumnType = "LVARCHAR";
                }
                else if (sourceColumnLength > 32739) {
                	targetColumnType = "TEXT";
                }
                else {
                    targetColumnType = "VARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("HDB")) {
                if (sourceColumnLength > 5000) {
                    targetColumnType = "CLOB";
                }
                else {
                    targetColumnType = "VARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
        	else {
    			targetColumnType = "VARCHAR";
    			if (targetProductName.toUpperCase().contains("MICROSOFT") && (sourceColumnLength > 8000)) {
    				targetColumnLength = -1;
    			}
    			else {
    				targetColumnLength = sourceColumnLength;
    			}
        	}
       	}
       	else if (
       			sourceColumnType.contains("DATE") ||
       			sourceColumnType.contains("TIME")
       	) {
       		if (targetProductName.toUpperCase().contains("ORACLE") || targetProductName.toUpperCase().contains("DB2")) {
           		targetColumnType = "DATE";
       		}
       		else if (targetProductName.toUpperCase().contains("POSTGRE")) {
           		targetColumnType = "TIMESTAMP";
       		}
       		else if (targetProductName.toUpperCase().contains("INFORMIX")) {
           		targetColumnType = "DATETIME YEAR TO FRACTION(3)";
       		}
       		else if (targetProductName.toUpperCase().contains("HDB")) {
           		targetColumnType = "SECONDDATE";
       		}
       		else {
       			targetColumnType = "DATETIME";
       		}
       	}
       	else if (
       			sourceColumnType.contains("NUM") ||
       			sourceColumnType.contains("BIN") ||
       			sourceColumnType.contains("DEC") ||
       			sourceColumnType.contains("INT") ||
       			sourceColumnType.contains("DOU") ||
       			sourceColumnType.contains("FLO") ||
       			sourceColumnType.contains("IDENT") ||
       			sourceColumnType.contains("MONEY")
       	) {
       		if (sourceColumnPrecision <= sourceColumnScale || sourceColumnScale < 0) {
       			targetColumnType = "FLOAT";
       		}
       		else {
       			if (targetProductName.toUpperCase().contains("ORACLE")) {
       				targetColumnType = "NUMBER";
       			}
       			else {
       				targetColumnType = "NUMERIC";
       			}
   				if (targetProductName.toUpperCase().contains("DB2") && sourceColumnPrecision > 31) {
       				targetColumnType = "DECFLOAT";
   				}
   				else {
           			targetColumnPrecision = sourceColumnPrecision;
           			targetColumnScale = sourceColumnScale;
   				}
       		}    		
       	}
       	else if (sourceColumnType.contains("MONEY")) {
       		if (targetProductName.toUpperCase().contains("MICROSOFT")) {
       			targetColumnType = "MONEY";
       		}
       		else {
       			if (targetProductName.toUpperCase().contains("ORACLE")) {
       				targetColumnType = "NUMBER";           				
       			}
       			else {
       				targetColumnType = "NUMERIC";
       			}
       			targetColumnPrecision = 22;
       			targetColumnScale = 5;
       		}    		
       	}
       	else if (
       			sourceColumnType.contains("BIT")
       	) {
       		if (targetProductName.toUpperCase().contains("POSTGRES")) {
   				targetColumnType = "BOOLEAN";           				
       		}
       		else if (targetProductName.toUpperCase().contains("ORACLE")) {
       				targetColumnType = "NUMBER";           				
       		}
       		else {
       			targetColumnType = "NUMERIC";
       		}	
       	}
       	else if (
       				sourceColumnType.contains("CLOB") ||
       				sourceColumnType.contains("TEXT")
       		) {
       		if (targetProductName.toUpperCase().contains("MYSQL")) {
        		targetColumnType = "LONGTEXT";
    		}
       		else if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		targetColumnType = "TEXT";
    		}
       		else if (targetProductName.toUpperCase().contains("MICROSOFT")) {
       			targetColumnType = "VARCHAR";
       			targetColumnLength = -1;
    		}
       		else if (targetProductName.toUpperCase().contains("ORACLE") || targetProductName.toUpperCase().contains("DB2")) {
        		targetColumnType = "CLOB";
    		}
       	}
       	else if (
   				sourceColumnType.contains("XML")
       		) {
       		if (targetProductName.toUpperCase().contains("MYSQL")) {
        		targetColumnType = "LONGTEXT";
    		}
       		else if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		targetColumnType = "TEXT";
    		}
       		else if (targetProductName.toUpperCase().contains("ORACLE")) {
        		targetColumnType = "XMLTYPE";
    		}
       		else {
        		targetColumnType = "XML";
    		}
       	}
       	else {
       		targetColumnType = sourceColumnType;
       		targetColumnLength = sourceColumnLength;
   			targetColumnPrecision = sourceColumnPrecision;
   			targetColumnScale = sourceColumnScale;
       	}
    	// Column definition
    	targetColumnDefinition = targetColumnType;
    	if (targetColumnPrecision > 0) {
    		targetColumnDefinition += "(" + targetColumnPrecision + "," +targetColumnScale + ")";
    	}
    	else if (targetColumnLength != 0) {
    		if (targetColumnLength==-1) {
    			targetColumnDefinition += "(max)";
    		}
    		else {
    			targetColumnDefinition += "(" + targetColumnLength + ")";
    		}
    	}
    }
}
