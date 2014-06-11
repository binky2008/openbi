package org.openbusinessintelligence.core.db;

import org.slf4j.LoggerFactory;

public class TypeConversionBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TypeConversionBean.class);

	private String sourceProductName;
    private String sourceColumnType;
    private String sourceColumnTypeAttribute;
    private int sourceColumnLength;
    private int sourceColumnPrecision;
    private int sourceColumnScale;
    private String sourceColumnDefinition;
    //
    private String targetProductName;
    private String targetColumnType;
    private String targetColumnTypeAttribute;
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
    	return targetColumnType;
    }
    public int getTargetColumnLength() {
    	return targetColumnLength;
    }
    public int getTargetColumnPrecision() {
    	return targetColumnPrecision;
    }
    public int getTargetColumnScale() {
    	return targetColumnScale;
    }
    public String getTargetColumnDefinition() {
    	return targetColumnDefinition;
    }
    
    // Conversion method
    public void convert() {

    	sourceColumnTypeAttribute = "";
    	targetColumnTypeAttribute = "";
    	sourceColumnDefinition = sourceColumnType;
    	
    	// Search for type attribute(s)    	
    	if (sourceColumnType.split(" ").length > 1) {
    		sourceColumnTypeAttribute = sourceColumnType.split(" ",2)[1];
    		sourceColumnType = sourceColumnType.split(" ",2)[0];
    	}
    	
    	// Source definition
    	if (sourceColumnPrecision > 0) {
    		sourceColumnDefinition += "(" + sourceColumnPrecision + "," + sourceColumnScale + ")";
    	}
    	else if (sourceColumnLength > 0) {
    		sourceColumnDefinition += "(" + sourceColumnLength + ")";
    	}
    	sourceColumnDefinition += " " + sourceColumnTypeAttribute;

   		targetColumnLength = 0;
   		targetColumnPrecision = 0;
   		targetColumnScale = 0;

       	//*******************************
    	// set target column properties
        // Same data type if RDBMSs are the same
   		if (targetProductName.toUpperCase().equals(sourceProductName.toUpperCase())) {
   			
       		targetColumnType = sourceColumnType;
       		targetColumnTypeAttribute = sourceColumnTypeAttribute;
       		
       		if (
       			targetProductName.toUpperCase().contains("MYSQL") &&
       			sourceColumnType.contains("CHAR")
       		) {
                if (sourceColumnLength > 255) {
                    targetColumnType = "LONGTEXT";
        			targetColumnLength = 0;
                }
                else {
                	targetColumnLength = sourceColumnLength;
                }
       		}
       		else if (
               	targetProductName.toUpperCase().contains("DB2") && (
               		sourceColumnType.contains("INTEGER") ||
               		sourceColumnType.contains("REAL")
           		)
           	) {
           		targetColumnLength = 0;
            }
       		else if (
       			sourceColumnType.contains("BOOL") ||
           		sourceColumnType.contains("TINY") ||
           		sourceColumnType.contains("SMALL") ||
           		sourceColumnType.contains("MEDIUM") ||
           		sourceColumnType.contains("BIG") ||
           		sourceColumnType.contains("LONG") ||
           		sourceColumnType.contains("SERIAL") ||
           		sourceColumnType.contains("REAL") ||
           		sourceColumnType.contains("FLOAT") ||
           		sourceColumnType.contains("DOUBLE") ||
           		sourceColumnType.contains("UNSIGNED") ||
           		sourceColumnType.contains("MONEY") ||
           		sourceColumnType.contains("TEXT") ||
           		sourceColumnType.contains("DATE") ||
           		sourceColumnType.contains("TIME") ||
           		sourceColumnType.contains("INTERVAL") ||
           		sourceColumnType.contains("YEAR") ||
           		sourceColumnType.contains("CLOB") ||
           		sourceColumnType.contains("BLOB") ||
           		sourceColumnType.contains("XML") ||
           		sourceColumnType.contains("JSON") ||
           		// PostgreSQL types
           		sourceColumnType.contains("INT2") ||
           		sourceColumnType.contains("INT4") ||
           		sourceColumnType.contains("INT8") ||
           		sourceColumnType.contains("FLOAT4") ||
           		sourceColumnType.contains("FLOAT8") ||
           		sourceColumnType.contains("BYTEA") ||
           		sourceColumnType.contains("TXID") ||
           		sourceColumnType.contains("UUID") ||
           		sourceColumnType.contains("CIDR") ||
           		sourceColumnType.contains("INET") ||
           		sourceColumnType.contains("MACADDR") ||
           		sourceColumnType.contains("TSQUERY") ||
           		sourceColumnType.contains("TSVECTOR") ||
           		sourceColumnType.contains("BOX") ||
           		sourceColumnType.contains("CIRCLE") ||
           		sourceColumnType.contains("LINE") ||
           		sourceColumnType.contains("LSEG") ||
           		sourceColumnType.contains("PATH") ||
           		sourceColumnType.contains("POINT") ||
           		sourceColumnType.contains("POLYGON") ||
    	   		// Oracle specific types
    	   		sourceColumnType.contains("SDO_GEOMETRY") ||
    	   		sourceColumnType.contains("SDO_RASTER") ||
           		sourceColumnType.contains("BFILE") ||
           		sourceColumnType.contains("ROWID") ||
           		// SQL Server specific types
               	sourceColumnType.contains("IMAGE") ||
               	sourceColumnType.contains("GEOGRAPHY") ||
               	sourceColumnType.contains("GEOMETRY") ||
               	sourceColumnType.contains("HIERARCHYID") ||
               	sourceColumnType.contains("UNIQUEIDENTIFIER") ||
           		// Informix specific types
               	sourceColumnType.contains("BYTE") ||
               	sourceColumnType.contains("BINARY18") ||
               	sourceColumnType.contains("BINARYVAR")
           	) {
       			// For TINY*, SMALL*, BIG* and DATE/TIME no length, precision and scale needed
       			if (targetProductName.toUpperCase().contains("ORACLE"))
       				if (sourceColumnType.contains("INTERVALDS")) {
       					targetColumnType = "INTERVAL DAY TO SECOND";
       				}
       				if (sourceColumnType.contains("INTERVALYM")) {
       					targetColumnType = "INTERVAL YEAR TO MONTH";
       			}
           		if (targetProductName.toUpperCase().contains("INFORMIX")) {
       				if (sourceColumnType.contains("DATETIME")) {
       					targetColumnType = "DATETIME YEAR TO FRACTION(3)";
       				}
       				if (sourceColumnType.contains("INTERVAL")) {
       					targetColumnType = "VARCHAR";
                        targetColumnLength = 255;
       				}
           		}
       		}
       		else if (
                sourceColumnType.contains("BIT") ||
                sourceColumnType.contains("INT")
            ) {
            	if (
            		targetProductName.toUpperCase().contains("MICROSOFT") ||
            		targetProductName.toUpperCase().contains("INFORMIX")
            	) {
                    targetColumnLength = 0;
                }
                else {
                    targetColumnLength = sourceColumnLength;
                }
           	}
       		else if (
       			sourceColumnType.contains("CHAR") ||
               	sourceColumnType.contains("BINAR")
            ) {
                if (
                    targetProductName.toUpperCase().contains("MICROSOFT") &&
                    sourceColumnLength > 8000
                ) {
                    targetColumnLength = -1;
                }
                else {
                   	targetColumnLength = sourceColumnLength;
                }
       		}
       		else if (sourceColumnType.contains("GRAPHIC")) {
       			targetColumnLength = sourceColumnLength;
           	}
       		else if (
       			targetProductName.toUpperCase().contains("ORACLE") &&
       			sourceColumnType.contains("NUMBER") &&
       			sourceColumnPrecision > 38
       		) {
       			targetColumnType = "FLOAT";
       		}
       		else {
           		targetColumnLength = sourceColumnLength;
	   			targetColumnPrecision = sourceColumnPrecision;
	   			targetColumnScale = sourceColumnScale;
	   			if (targetColumnScale > targetColumnPrecision) {
	   				targetColumnScale = 0;
	   			}
       		}
   		}
   		// Oracle special types
        else if (sourceColumnType.contains("ROWID")) {
        	targetColumnType = "VARCHAR";
			targetColumnLength = 255;
        }
        else if (
	   		sourceColumnType.contains("SDO_GEOMETRY") ||
	   		sourceColumnType.contains("SDO_RASTER")
   		) {
        	if (
        		targetProductName.toUpperCase().contains("DB2") ||
        		targetProductName.toUpperCase().contains("HDB") ||
        		targetProductName.toUpperCase().contains("TERADATA")
        	) {
        		targetColumnType = "CLOB";
        	}
        	else {
        		targetColumnType = "TEXT";
        	}
			targetColumnLength = 0;
        }
   		// PostgreSQL special types
        else if (
	   		sourceColumnType.contains("TXID") ||
	   		sourceColumnType.contains("UUID") ||
	   		sourceColumnType.contains("CIDR") ||
	   		sourceColumnType.contains("INET") ||
	   		sourceColumnType.contains("MACADDR")
   		) {
        	if (targetProductName.toUpperCase().contains("ORACLE")) {
        		targetColumnType = "VARCHAR2";
        	}
        	else {
        		targetColumnType = "VARCHAR";
        	}
			targetColumnLength = 255;
        }
        else if (
	   		sourceColumnType.contains("TSQUERY") ||
	   		sourceColumnType.contains("TSVECTOR") ||
	   		sourceColumnType.contains("BOX") ||
	   		sourceColumnType.contains("CIRCLE") ||
	   		sourceColumnType.contains("LINE") ||
	   		sourceColumnType.contains("LSEG") ||
	   		sourceColumnType.contains("PATH") ||
	   		sourceColumnType.contains("POINT") ||
	   		sourceColumnType.contains("POLYGON")
   		) {
        	if (
        		targetProductName.toUpperCase().contains("ORACLE") ||
        		targetProductName.toUpperCase().contains("DB2") ||
        		targetProductName.toUpperCase().contains("HDB") ||
       			targetProductName.toUpperCase().contains("TERADATA")
        	) {
        		targetColumnType = "CLOB";
        	}
        	else {
        		targetColumnType = "TEXT";
        	}
        }
   		// SQL Server special types
        else if (
        	sourceProductName.toUpperCase().contains("MICROSOFT") &&
	   		sourceColumnType.contains("TIMESTAMP")
   		) {
       		if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		targetColumnType = "BYTEA";
    		}
       		else if (targetProductName.toUpperCase().contains("INFORMIX")) {
        		targetColumnType = "BYTE";
    		}
       		else if (targetProductName.toUpperCase().contains("MICROSOFT")) {
        		targetColumnType = "VARBINARY";
        		targetColumnLength = -1;
    		}
       		else {
        		targetColumnType = "BLOB";
    		}
        }
        else if (
	   		sourceColumnType.contains("HIERARCHYID") ||
	   		sourceColumnType.contains("UNIQUEIDENTIFIER")
   		) {
        	if (targetProductName.toUpperCase().contains("ORACLE")) {
        		targetColumnType = "VARCHAR2";
        	}
        	else {
        		targetColumnType = "VARCHAR";
        	}
			targetColumnLength = 255;
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
                if (sourceColumnLength > 2000) {
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
            else if (targetProductName.toUpperCase().contains("TERADATA")) {
                if (sourceColumnLength > 4000) {
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
                    targetColumnType = "NVARCHAR";
                    targetColumnLength = sourceColumnLength;
                }
            }
            else if (targetProductName.toUpperCase().contains("INFORMIX")) {
                if ((sourceColumnLength > 255) && (sourceColumnLength <= 32739)) {
                    targetColumnType = "LVARCHAR";
        			targetColumnLength = sourceColumnLength;
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
                if (
                	targetProductName.toUpperCase().contains("MICROSOFT") &&
                	sourceColumnLength > 4000
                ) {
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
    	else if (
    		(sourceColumnType.contains("CHAR") && sourceColumnLength > 1) ||
    		sourceColumnType.contains("GRAPHIC")
    	) {
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
            else if (targetProductName.toUpperCase().contains("TERADATA")) {
                if (sourceColumnLength > 4000) {
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
    			if (
    				targetProductName.toUpperCase().contains("MICROSOFT") &&
    				sourceColumnLength > 4000
    			) {
    				targetColumnLength = -1;
    			}
    			else {
    				targetColumnLength = sourceColumnLength;
    			}
        	}
       		targetColumnPrecision = 0;
       		targetColumnScale = 0;
       	}
   		// Big text types
       	else if (
       			sourceColumnType.contains("CLOB") ||
       			sourceColumnType.contains("TEXT") ||
       			sourceColumnType.contains("JSON")
       		) {
       		if (targetProductName.toUpperCase().contains("MYSQL")) {
        		targetColumnType = "LONGTEXT";
           		targetColumnLength = 0;
    		}
       		else if  (
           		targetProductName.toUpperCase().contains("POSTGRES") ||
           		targetProductName.toUpperCase().contains("INFORMIX")
           	) {
        		targetColumnType = "TEXT";
           		targetColumnLength = 0;
    		}
       		else if (targetProductName.toUpperCase().contains("MICROSOFT")) {
       			targetColumnType = "VARCHAR";
       			targetColumnLength = -1;
    		}
       		else if (
       			targetProductName.toUpperCase().contains("ORACLE") ||
       			targetProductName.toUpperCase().contains("DB2")
       		) {
        		targetColumnType = "CLOB";
           		targetColumnLength = 0;
    		}
       		else {
        		targetColumnType = "CLOB";
           		targetColumnLength = 0;
       		}
       	}
   		// XML type
       	else if (sourceColumnType.contains("XML")) {
       		if (targetProductName.toUpperCase().contains("MYSQL")) {
        		targetColumnType = "LONGTEXT";
    		}
       		else if (
       			targetProductName.toUpperCase().contains("POSTGRES") ||
       			targetProductName.toUpperCase().contains("INFORMIX")
       			) {
        		targetColumnType = "TEXT";
    		}
       		else if (targetProductName.toUpperCase().contains("ORACLE")) {
        		targetColumnType = "XMLTYPE";
    		}
       		else if (
       			targetProductName.toUpperCase().contains("HDB") ||
           		targetProductName.toUpperCase().contains("TERADATA")
           	) {
        		targetColumnType = "CLOB";
    		}
       		else {
        		targetColumnType = "XML";
    		}
       	}
   		// Date/time types
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
       		else if (targetProductName.toUpperCase().contains("TERADATA")) {
           		targetColumnType = "TIMESTAMP";
       		}
       		else {
       			targetColumnType = "DATETIME";
       		}
       		targetColumnLength = 0;
       		targetColumnPrecision = 0;
       		targetColumnScale = 0;
       	}
   		// Interval type
       	else if (sourceColumnType.contains("INTERVAL")) {
       		if (targetProductName.toUpperCase().contains("POSTGRES")) {
           		targetColumnType = "INTERVAL";
       		}
       		else if (targetProductName.toUpperCase().contains("ORACLE")) {
           		targetColumnType = "VARCHAR2";
           		targetColumnLength = 255;
       		}
       		else {
           		targetColumnType = "VARCHAR";
           		targetColumnLength = 255;
       		}
       	}
   		// Double types
       	else if (sourceColumnType.contains("DOUBLE") ||
       			sourceColumnType.contains("REAL")) {
   			if (targetProductName.toUpperCase().contains("ORACLE")) {
   				targetColumnType = "BINARY_DOUBLE";
   			}
   			else if (targetProductName.toUpperCase().contains("MICROSOFT")) {
   				targetColumnType = "FLOAT";
   			}
   			else if (
   				targetProductName.toUpperCase().contains("POSTGRES") ||
   				targetProductName.toUpperCase().contains("INFORMIX") ||
   				targetProductName.toUpperCase().contains("TERADATA")
   			) {
   				targetColumnType = "DOUBLE PRECISION";
   			}
   			else {
   	       		targetColumnType = "DOUBLE";
   			}
       	}
   		// Numeric types
       	else if (
       			sourceColumnType.contains("NUMBER") ||
       			sourceColumnType.contains("NUMERIC") ||
       			sourceColumnType.contains("SERIAL") ||
       			sourceColumnType.contains("DEC") ||
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
       			else if (targetProductName.toUpperCase().contains("MYSQL") && sourceColumnPrecision > 30) {
       				targetColumnType = "FLOAT";
   				}
       			else if (targetProductName.toUpperCase().contains("DB2") && sourceColumnPrecision > 31) {
       				targetColumnType = "DECFLOAT";
   				}
       			else if (targetProductName.toUpperCase().contains("TERADATA") && sourceColumnPrecision > 38) {
       				targetColumnType = "FLOAT";
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
       			else if (
       				targetProductName.toUpperCase().contains("POSTGRES") &&
       				sourceColumnType.contains("SERIAL")
       			) {
       				targetColumnType = "SERIAL";
   				}
       			else {
       				targetColumnType = "NUMERIC";
           			targetColumnPrecision = sourceColumnPrecision;
           			targetColumnScale = sourceColumnScale;
       			}
       		}    		
       	}
   		// Money type
       	else if (sourceColumnType.contains("MONEY")) {
       		if (
       			targetProductName.toUpperCase().contains("MICROSOFT") ||
       			targetProductName.toUpperCase().contains("POSTGRES") ||
       			targetProductName.toUpperCase().contains("INFORMIX")
       		) {
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
   		// Bit type
       	else if (sourceColumnType.contains("BIT")) {
       		if (
       			targetProductName.toUpperCase().contains("MYSQL") ||
       			targetProductName.toUpperCase().contains("POSTGRES")
       		) {
   				targetColumnType = "BIT";
   				targetColumnLength = sourceColumnLength;
       		}
       		else if (targetProductName.toUpperCase().contains("ORACLE")) {
       			targetColumnType = "NUMBER";
   				targetColumnLength = sourceColumnLength;
       		}
       		else {
       			targetColumnType = "NUMERIC";
   				targetColumnLength = sourceColumnLength;
       		}	
       	}
   		// Boolean type
       	else if (sourceColumnType.contains("BOOL")) {
   			if (targetProductName.toUpperCase().contains("ORACLE")) {
   				targetColumnType = "NUMBER";
   	   			targetColumnPrecision = sourceColumnPrecision;
   	   			targetColumnScale = sourceColumnScale;
   			}
   			else if (
   				targetProductName.toUpperCase().contains("DB2") ||
   				targetProductName.toUpperCase().contains("HDB") ||
   				targetProductName.toUpperCase().contains("MICROSOFT") ||
   				targetProductName.toUpperCase().contains("TERADATA")
   	   	   	) {
   	   			targetColumnType = "NUMERIC";
   	   	   		targetColumnPrecision = sourceColumnPrecision;
   	   	   		targetColumnScale = sourceColumnScale;
   	   		}
   			else {
   				targetColumnType = "BOOLEAN";
   			}
       	}
   		// BLOB types
       	else if (
       		sourceColumnType.contains("BLOB") ||
       		sourceColumnType.contains("BYTE") ||
       		sourceColumnType.contains("BFILE") ||
       		sourceColumnType.contains("LONG") ||
       		sourceColumnType.contains("IMAGE") ||
       		sourceColumnType.contains("BINARY") ||
       		sourceColumnType.contains("GEOGRAPHY") ||
       		sourceColumnType.contains("GEOMETRY")
       	) {
       		if (targetProductName.toUpperCase().contains("POSTGRES")) {
        		targetColumnType = "BYTEA";
    		}
       		else if (targetProductName.toUpperCase().contains("INFORMIX")) {
        		targetColumnType = "BYTE";
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
    	targetColumnDefinition += " " + targetColumnTypeAttribute;
    }
}
