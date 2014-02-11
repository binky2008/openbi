package org.openbusinessintelligence.core.db;

public class TypeConversionBean {

    private String sourceProductName;
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
    public void setSourceProductName(String property) {
    	sourceProductName = property;
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

   		targetColumnLength = 0;
   		targetColumnPrecision = 0;
   		targetColumnScale = 0;

       	//*******************************
    	// set target column properties
        // Same data type if RDBMSs are the same
   		if (targetProductName.toUpperCase().equals(sourceProductName.toUpperCase())) {
       		targetColumnType = sourceColumnType;
       		if (targetProductName.toUpperCase().contains("MYSQL") && sourceColumnType.contains("CHAR")) {
                if (sourceColumnLength > 255) {
                    targetColumnType = "LONGTEXT";
        			targetColumnLength = 0;
                }
                else {
                	targetColumnLength = sourceColumnLength;
                }
       		}
       		else if (
           			sourceColumnType.contains("TINY") ||
           			sourceColumnType.contains("SMALL") ||
           			sourceColumnType.contains("MEDIUM") ||
           			sourceColumnType.contains("BIG") ||
           			sourceColumnType.contains("LONG") ||
           			sourceColumnType.contains("SERIAL") ||
           			sourceColumnType.contains("DATE") ||
           			sourceColumnType.contains("TIME") ||
           			sourceColumnType.contains("YEAR") ||
           			sourceColumnType.contains("CLOB") ||
           			sourceColumnType.contains("BLOB")
           	) {
       			// For TINY*, SMALL*, BIG* and DATE/TIME no length, precision and scale needed
       		}
       		else if (sourceColumnType.contains("CHAR") ||
               		sourceColumnType.contains("BIT") ||
               		sourceColumnType.contains("INT") ||
               		sourceColumnType.contains("BINAR")
            ) {
               	targetColumnLength = sourceColumnLength;
       		}
       		else {
           		targetColumnLength = sourceColumnLength;
	   			targetColumnPrecision = sourceColumnPrecision;
	   			targetColumnScale = sourceColumnScale;
       		}
   		}
        // NCHAR and NVARCHAR types
   		else if (sourceColumnType.contains("NCHAR") && sourceColumnLength == 1) {
            targetColumnType = "NCHAR";
			targetColumnLength = 0;
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
        			targetColumnLength = 0;
                }
                else {
                    targetColumnType = "NVARCHAR2";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("DB2")) {
                if (sourceColumnLength > 32672) {
                    targetColumnType = "CLOB";
        			targetColumnLength = 0;
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("POSTGRES")) {
                if (sourceColumnLength > 10000000) {
                    targetColumnType = "TEXT";
        			targetColumnLength = 0;
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("MYSQL")) {
                if (sourceColumnLength > 255) {
                    targetColumnType = "LONGTEXT";
        			targetColumnLength = 0;
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("INFORMIX")) {
                if ((sourceColumnLength > 255) && (sourceColumnLength <= 32739)) {
                    targetColumnType = "LVARCHAR";
        			targetColumnLength = 0;
                }
                else if (sourceColumnLength > 32739) {
                	targetColumnType = "TEXT";
        			targetColumnLength = 0;
                }
                else {
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("HDB")) {
                if (sourceColumnLength > 5000) {
                    targetColumnType = "CLOB";
        			targetColumnLength = 0;
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
       		targetColumnPrecision = 0;
       		targetColumnScale = 0;
        }
        // CHAR and VARCHAR types
    	else if (sourceColumnType.contains("CHAR") && sourceColumnLength == 1) {
       		targetColumnType = "CHAR (1)";
			targetColumnLength = 0;
       	}
    	else if ((sourceColumnType.contains("CHAR") && sourceColumnLength > 1)) {
    		if (targetProductName.toUpperCase().contains("ORACLE")) {
        		if (sourceColumnLength > 4000) {
        			targetColumnType = "CLOB";
        			targetColumnLength = 0;
        		}
        		else {
        			targetColumnType = "VARCHAR2";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
    		else if (targetProductName.toUpperCase().contains("DB2")) {
        		if (sourceColumnLength > 32672) {
        			targetColumnType = "CLOB";
        			targetColumnLength = 0;
        		}
        		else {
        			targetColumnType = "VARCHAR";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
    		else if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		if (sourceColumnLength > 10000000) {
        			targetColumnType = "TEXT";
        			targetColumnLength = 0;
        		}
        		else {
        			targetColumnType = "VARCHAR";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
    		else if (targetProductName.toUpperCase().contains("MYSQL")) {
        		if (sourceColumnLength > 255) {
        			targetColumnType = "LONGTEXT";
        			targetColumnLength = 0;
        		}
        		else {
        			targetColumnType = "VARCHAR";
        			targetColumnLength = sourceColumnLength;
        		}
    		}
            else if (targetProductName.toUpperCase().contains("INFORMIX")) {
                if ((sourceColumnLength > 255) && (sourceColumnLength <= 32739)) {
                    targetColumnType = "LVARCHAR";
        			targetColumnLength = 0;
                }
                else if (sourceColumnLength > 32739) {
                	targetColumnType = "TEXT";
        			targetColumnLength = 0;
                }
                else {
                    targetColumnType = "VARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("HDB")) {
                if (sourceColumnLength > 5000) {
                    targetColumnType = "CLOB";
        			targetColumnLength = 0;
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
       		targetColumnPrecision = 0;
       		targetColumnScale = 0;
       	}
       	else if (
       			sourceColumnType.contains("DATE") ||
       			sourceColumnType.contains("TIME") ||
       			sourceColumnType.contains("YEAR")
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
       		targetColumnLength = 0;
       		targetColumnPrecision = 0;
       		targetColumnScale = 0;
       	}
       	else if (sourceColumnType.contains("DOUBLE")) {
   			if (targetProductName.toUpperCase().contains("ORACLE")) {
   				targetColumnType = "BINARY_DOUBLE";
   			}
   			else if (targetProductName.toUpperCase().contains("MICROSOFT")) {
   				targetColumnType = "FLOAT";
   			}
   			else if (
   				targetProductName.toUpperCase().contains("POSTGRES") ||
   				targetProductName.toUpperCase().contains("INFORMIX")
   			) {
   				targetColumnType = "DOUBLE PRECISION";
   			}
   			else {
   	       		targetColumnType = "DOUBLE";
   			}
       	}
       	else if (
       			sourceColumnType.contains("NUMBER") ||
       			sourceColumnType.contains("NUMERIC") ||
       			sourceColumnType.contains("DEC") ||
       			sourceColumnType.contains("BIN") ||
       			sourceColumnType.contains("INT") ||
       			sourceColumnType.contains("FLO") ||
       			sourceColumnType.contains("IDENT")
       	) {
       		if (sourceColumnPrecision <= sourceColumnScale || sourceColumnScale < 0) {
       			targetColumnType = "FLOAT";
       		}
       		else {
       			if (targetProductName.toUpperCase().contains("ORACLE")) {
       				if (sourceColumnPrecision > 38) {
	       				targetColumnType = "FLOAT";
       				}
       				else {
	       				targetColumnType = "NUMBER";
	           			targetColumnPrecision = sourceColumnPrecision;
	           			targetColumnScale = sourceColumnScale;
	       			}
       			}
       			else if (targetProductName.toUpperCase().contains("DB2") && sourceColumnPrecision > 31) {
       				targetColumnType = "DECFLOAT";
   				}
       			else if (targetProductName.toUpperCase().contains("MICROSOFT") && sourceColumnPrecision > 38) {
       				targetColumnType = "FLOAT";
   				}
       			else if (targetProductName.toUpperCase().contains("HDB") && sourceColumnPrecision > 38) {
       				targetColumnType = "FLOAT";
   				}
       			else if (targetProductName.toUpperCase().contains("INFORMIX")) {
       				if (sourceColumnPrecision > 32) {
           				targetColumnType = "FLOAT";
       				}
       				else {
	       				targetColumnType = "DECIMAL";
	           			targetColumnPrecision = sourceColumnPrecision;
	           			targetColumnScale = sourceColumnScale;
	       			}
   				}
       			else {
       				targetColumnType = "NUMERIC";
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
           		targetColumnLength = 0;
    		}
       		else if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		targetColumnType = "TEXT";
           		targetColumnLength = 0;
    		}
       		else if (targetProductName.toUpperCase().contains("MICROSOFT")) {
       			targetColumnType = "VARCHAR";
       			targetColumnLength = -1;
    		}
       		else if (targetProductName.toUpperCase().contains("ORACLE") || targetProductName.toUpperCase().contains("DB2")) {
        		targetColumnType = "CLOB";
           		targetColumnLength = 0;
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
       	else if (sourceColumnType.contains("BLOB")) {
       		if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		targetColumnType = "BYTEA";
    		}
       		else if (targetProductName.toUpperCase().contains("MICROSOFT")) {
        		targetColumnType = "VARBINARY";
        		targetColumnLength = -1;
    		}
       		else {
        		targetColumnType = "BLOB";
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
