package org.openbusinessintelligence.core.db;

import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class TypeConversionBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TypeConversionBean.class);

	org.w3c.dom.Document convertionMatrix = null;
	
    private int columnJdbcType = 0;
	
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
    
    // conversion matrix
    private NodeList nSourceTypeList;
    private NodeList nSourceSubTypeList;
    private NodeList nTargetProductList;
    private NodeList nTargetTypeList;
    private NodeList nTargetSubTypeList;
    
    private Node nNode;
    private Element eSourceTypeElement;
    private Element eProductElement;
    private Element eSubTypeElement;
    
    private String matchedTypeID = "";
    private String matchedTypeName = "";
    
    private String targetProductType = "";
    private String targetDefaultType = "";
    private int targetMaxLength = 0;
    
    private boolean typeMatch = false;
    private boolean productTypeMatch = false;
    private boolean lengthOption = true;
    private boolean isOversized = true;
    
    // Setter methods
    public void setConvertionMatrix(org.w3c.dom.Document property) {
    	convertionMatrix = property;
    }
    public void setColumnJdbcType(int property) {
    	columnJdbcType = property;
    }
    public void setSourceColumnType(String property) {
    	sourceColumnType = property;
    }
    public void setSourceColumnTypeAttribute(String property) {
    	sourceColumnTypeAttribute = property;
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
    public String getTargetColumnTypeAttribute() {
    	return targetColumnTypeAttribute;
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

    	targetColumnTypeAttribute = "";
    	sourceColumnDefinition = sourceColumnType;
    	
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
   		
   		/**
   		 * Source and target DB products are the same
   		 */
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
       		else if (sourceColumnType.contains("OTHER")) {
       			targetColumnType = "OTHER";
       		}
       		else if (
       			sourceColumnType.contains("ALPHANUM") &&
           		targetProductName.toUpperCase().contains("HDB")
       		) {
                targetColumnPrecision = 0;
                targetColumnScale = 0;
                targetColumnLength = sourceColumnLength;
       		}
       		else if (
                sourceColumnType.contains("BIT") ||
                sourceColumnType.contains("INT")
            ) {
            	if (
            		targetProductName.toUpperCase().contains("MICROSOFT") ||
            		targetProductName.toUpperCase().contains("INFORMIX") ||
            		targetProductName.toUpperCase().contains("DERBY") ||
            		targetProductName.toUpperCase().contains("FIREBIRD") ||
            		targetProductName.toUpperCase().contains("HDB")
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
       		// MySQL special types
            else if (
            	sourceColumnType.equalsIgnoreCase("ENUM") ||
            	sourceColumnType.equalsIgnoreCase("SET")
            ) {
            	targetColumnType = "VARCHAR";
    			targetColumnLength = sourceColumnLength;
    			targetColumnPrecision = 0;
    			targetColumnScale = 0;
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
   		
   		/**
   		 * Source and target DB products are different
   		 */
   		else {
   	        
   			nSourceTypeList = convertionMatrix.getElementsByTagName("jdbcType");
   	        
   	        matchedTypeID = "";
   	        matchedTypeName = "";
   	        targetProductType = "";
   	        targetMaxLength = 0;
   	        isOversized = false;
   	        lengthOption = true;
   			
   			// Scroll source types in the matrix
   	 		for (int t = 0; t < nSourceTypeList.getLength(); t++) {
   	 			nNode = nSourceTypeList.item(t);
   	 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	 				eSourceTypeElement = (Element) nNode;

   	 				// Match by type name
   	 				nSourceSubTypeList = eSourceTypeElement.getChildNodes();
   	 				for (int s = 0; s < nSourceSubTypeList.getLength(); s++) {
   	     				if (
   	         				nSourceSubTypeList.item(s).getNodeName().equals("typeName") &&
   	     					nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(sourceColumnType))
   	     				) {
   	     					// Jdbc type match by name
   	     					matchedTypeName = nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue();
   	     					logger.debug("SOURCE TYPE NAME = " + matchedTypeName);
   	     				}
   	     				else if (
   	             			nSourceSubTypeList.item(s).getNodeName().equals("typeId") &&
   	         				nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(columnJdbcType))
   	         			) {
   	         				// Jdbc type match by id
   	         				matchedTypeID = nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue();
   	         				logger.debug("JDBC TYPE ID = " + matchedTypeID);
   	     				}
   	 				}
   	 			}
   	 		}
   	 		
   	 		for (int t = 0; t < nSourceTypeList.getLength(); t++) {
   	 			typeMatch = false;
   	 			nNode = nSourceTypeList.item(t);
   	 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	 				eSourceTypeElement = (Element) nNode;
   	     			nSourceSubTypeList = eSourceTypeElement.getChildNodes();
   	     			for (int s = 0; s < nSourceSubTypeList.getLength(); s++) {
   	         			if (
   	             			nSourceSubTypeList.item(s).getNodeName().equals("typeName") &&
   	         				nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(sourceColumnType))
   	         			) {
   	         				// Jdbc type match by name
   		         			typeMatch = true;
   	         				logger.debug("SOURCE MATCH BY NAME = " + nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue());
   	         			}
   	         			else if (
   	         				matchedTypeName.equals("") &&
   	                   		nSourceSubTypeList.item(s).getNodeName().equals("typeId") &&
   	                		nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(columnJdbcType))
   	                	) {
   	    	        		// Jdbc type match by id
   		         			typeMatch = true;
   	    	        		logger.debug("SOURCE MATCH BY ID = " + nSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue());
   	             		}
   	     			}
   	 				
   	 				if (typeMatch) {
   	   	     			
   	   	     			logger.debug("SOURCE LENGTH = " + sourceColumnLength);
   	 					productTypeMatch = false;
   	 					nTargetProductList = eSourceTypeElement.getElementsByTagName("DBType");
   	 					
   	 		    		// Scroll db products in the matrix
   	 		     		for (int p = 0; p < nTargetProductList.getLength(); p++) {
   	 		     			nNode = nTargetProductList.item(p);
   	 		     			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	 		     				eProductElement = (Element) nNode;
   	 		     				if (targetProductName.toUpperCase().contains(eProductElement.getElementsByTagName("productName").item(0).getChildNodes().item(0).getNodeValue())) {
   	 		     					// Product name match
   	 		     					logger.debug("PRODUCT MATCH");
   	 		     					
   	 		     					// Search for the type name
   	 		     					nTargetTypeList = eProductElement.getChildNodes();
   	 		     					//logger.debug("NO. OF TYPES = " + nTargetTypeList.getLength());
   	 		        				for (int tt = 0; tt < nTargetTypeList.getLength(); tt++) {
   	 		     		     			lengthOption = true;
   	     		     					if (nTargetTypeList.item(tt).getNodeName().equals("typeName")) {
   	            		     				productTypeMatch = true;
   	         		     					targetProductType = nTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue();
   	         		     					//logger.debug("PRODUCT TARGET TYPE = " + targetProductType);
   	     		     					}
   	     		     					else if (nTargetTypeList.item(tt).getNodeName().equals("maxLength")) {
   	     		     						targetMaxLength = Integer.valueOf(nTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
   	     		     						//logger.debug("PRODUCT MAX LENGTH = " + targetMaxLength);
   	   	     		     					if (targetMaxLength > 0 && targetMaxLength < sourceColumnLength) {
   	   	     		     						isOversized = true;
   	   	     		     						//logger.debug("IS OVERSIZED = " + isOversized);
   	   	     		     					}
   	     		     					}
   	   	     		     				else if (nTargetTypeList.item(tt).getNodeName().equals("lengthOption")) {
   	   	     		     					lengthOption = Boolean.valueOf(nTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
   	   	         		     				//logger.debug("LENGTH OPTION = " + lengthOption);
   	   	     		     				}
   	 		        				}
   	 		     					
   	 		     					// Search for sub type
   	 		     					nTargetSubTypeList = eProductElement.getElementsByTagName("SubType");
   	 		     					//logger.debug("NO. OF SUBTYPES = " + nTargetSubTypeList.getLength());
   	 		     		     		for (int st = 0; st < nTargetSubTypeList.getLength(); st++) {
   	 		     		     			nNode = nTargetSubTypeList.item(st);
   	     		     		     		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	     	     		     				eSubTypeElement = (Element) nNode;
	             		     				//logger.debug("MAX LENGTH = " + eSubTypeElement.getElementsByTagName("maxLength").item(0).getChildNodes().item(0).getNodeValue());
   	         		     					if (
   	         		     						!productTypeMatch &&
   	         		     						Integer.valueOf(eSubTypeElement.getElementsByTagName("maxLength").item(0).getChildNodes().item(0).getNodeValue()) >= sourceColumnLength
   	         		     					) {
   	             		     					productTypeMatch = true;
   	             		     					isOversized = false;
   	    	 		     		     			lengthOption = true;
   	         		     						nTargetTypeList = eSubTypeElement.getChildNodes();
	   	         		     					for (int tt = 0; tt < nTargetTypeList.getLength(); tt++) {
	   	    	     		     					if (nTargetTypeList.item(tt).getNodeName().equals("typeName")) {
	   	   	             		     					targetProductType = nTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue();
	   	   	             		     					//logger.debug("PRODUCT TARGET TYPE = " + targetProductType);
	   	    	     		     					}
	   	    	     		     					else if (nTargetTypeList.item(tt).getNodeName().equals("lengthOption")) {
	   	    	   	     		     					lengthOption = Boolean.valueOf(nTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
	   	    	   	         		     				//logger.debug("LENGTH OPTION = " + lengthOption);
	   	    	     		     					}
	   	         		     					}
   	     		     		     			}
   	         		     					else if (!productTypeMatch){
   	         		     						isOversized = true;
   	         		     					}
   	     		     		     		}
   	 		     		     		}
  	     		     				logger.debug("IS OVERSIZED = " + isOversized);
   	 		     		     		
   	 		     		     		// If size bigger than allowed max
   	 		     		     		if (isOversized) {
   	 		     		     			lengthOption = false;
   	   	 		     					// Search for the oversize properties
   	   	 		        				for (int tt = 0; tt < nTargetTypeList.getLength(); tt++) {
   	   	     		     					if (nTargetTypeList.item(tt).getNodeName().equals("typeName")) {
   	   	         		     					targetProductType = eProductElement.getElementsByTagName("oversizeTypeName").item(0).getChildNodes().item(0).getNodeValue();
   	   	         		     					//logger.debug("OVERSIZE TARGET TYPE = " + targetProductType);
   	   	     		     					}
   	   	     		     					else if (nTargetTypeList.item(tt).getNodeName().equals("oversizeLengthOption")) {
   	   	     		     						lengthOption = Boolean.valueOf(eProductElement.getElementsByTagName("oversizeLengthOption").item(0).getChildNodes().item(0).getNodeValue());
   	   	         		     					//logger.debug("OVERSIZE LENGTH OPTION = " + lengthOption);
   	   	     		     					}
   	   	 		        				}
   	 		     		     		}
   	 		     				}
   	 		     				else if (eProductElement.getElementsByTagName("productName").item(0).getChildNodes().item(0).getNodeValue().equalsIgnoreCase("DEFAULT")) {
   	 		     					// Product name match
   	 		     					targetDefaultType = eProductElement.getElementsByTagName("typeName").item(0).getChildNodes().item(0).getNodeValue();
   	 		     					logger.debug("DEFAULT TARGET TYPE = " + targetDefaultType);
   	 		     				}
   	 		     			}
   	 		     		}
   	 				}
   	 			}
   			}
   	 		
   	 		if (targetProductType.equals("")) {
   	 			targetProductType = targetDefaultType;
   	 		}
   	 		logger.debug("TARGET TYPE = " + targetProductType);
   	 		logger.debug("SIZE OPTION = " + lengthOption);
	   		
	   		// MySQL special types
	        if (
	        	sourceColumnType.equalsIgnoreCase("ENUM") ||
	        	sourceColumnType.equalsIgnoreCase("SET")
	        ) {
	        	targetColumnType = "VARCHAR";
				targetColumnLength = sourceColumnLength;
				targetColumnPrecision = 0;
				targetColumnScale = 0;
	        }
	   		// HANA special types
	        else if (sourceColumnType.contains("ALPHANUM")) {
	        	targetColumnType = "VARCHAR";
				targetColumnLength = sourceColumnLength;
	        }
	   		// Oracle special types
	        else if (sourceColumnType.contains("ROWID")) {
	        	targetColumnType = "VARCHAR";
				targetColumnLength = 255;
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
	            else if (targetProductName.toUpperCase().contains("DERBY")) {
	                if (sourceColumnLength > 32672) {
	                    targetColumnType = "CLOB";
	        			targetColumnLength = 0;
	                }
	                else {
	                    targetColumnType = "VARCHAR";
	                    targetColumnLength = sourceColumnLength;
	                }
	            }
	            else if (targetProductName.toUpperCase().contains("FIREBIRD")) {
	                if (sourceColumnLength > 4000) {
	                    targetColumnType = "BLOB";
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
	                    targetColumnType = "NVARCHAR";
	                    targetColumnLength = sourceColumnLength;
	                }
	            }
	            else if (targetProductName.toUpperCase().contains("ANYWHERE")) {
	                if (sourceColumnLength > 32767) {
	                    targetColumnType = "TEXT";
	        			targetColumnLength = 0;
	                }
	                else {
	                    targetColumnType = "NVARCHAR";
	                    targetColumnLength = sourceColumnLength;
	                }
	            }
	            else if (targetProductName.toUpperCase().contains("VERTICA")) {
	                if (sourceColumnLength > 65000) {
	                    targetColumnType = "LONG VARCHAR";
	                    if (sourceColumnLength > 2000000) {
	                    	targetColumnLength = 2000000;
	                    }
	                    else {
	                    	targetColumnLength = sourceColumnLength;
	                    }
	                }
	                else {
	                    targetColumnType = "VARCHAR";
	                    targetColumnLength = sourceColumnLength;
	                }
	            }
	            else if (targetProductName.toUpperCase().contains("NETEZZA")) {
	                targetColumnType = "NVARCHAR";
	                if (sourceColumnLength > 16000) {
	        			targetColumnLength = 16000;
	                }
	                else {
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
	            else if (targetProductName.toUpperCase().contains("DERBY")) {
	                if (sourceColumnLength > 32672) {
	                    targetColumnType = "CLOB";
	        			targetColumnLength = sourceColumnLength;
	                }
	                else {
	                    targetColumnType = "VARCHAR";
	                    targetColumnLength = sourceColumnLength;
	                }
	            }
	            else if (targetProductName.toUpperCase().contains("FIREBIRD")) {
	                if (sourceColumnLength > 4000) {
	                    targetColumnType = "BLOB";
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
	            else if (targetProductName.toUpperCase().contains("ANYWHERE")) {
	                if (sourceColumnLength > 32767) {
	                    targetColumnType = "TEXT";
	        			targetColumnLength = 0;
	                }
	                else {
	                    targetColumnType = "VARCHAR";
	                    targetColumnLength = sourceColumnLength;
	                }
	            }
	            else if (targetProductName.toUpperCase().contains("VERTICA")) {
	                if (sourceColumnLength > 65000) {
	                    targetColumnType = "LONG VARCHAR";
	                    if (sourceColumnLength > 2000000) {
	                    	targetColumnLength = 2000000;
	                    }
	                    else {
	                    	targetColumnLength = sourceColumnLength;
	                    }
	                }
	                else {
	                    targetColumnType = "VARCHAR";
	                    targetColumnLength = sourceColumnLength;
	                }
	            }
	            else if (targetProductName.toUpperCase().contains("NETEZZA")) {
	                targetColumnType = "VARCHAR";
	                if (sourceColumnLength > 1000) {
	        			targetColumnLength = 1000;
	                }
	                else {
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
	       			sourceColumnType.contains("JSON") ||
	    	   		sourceColumnType.contains("SDO_GEOMETRY") ||
	    	   		sourceColumnType.contains("SDO_RASTER")
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
	       		else if (targetProductName.toUpperCase().contains("NETEZZA")) {
	       			targetColumnType = "VARCHAR";
	       			targetColumnLength = 1000;
	    		}
	       		else if (
	       			targetProductName.toUpperCase().contains("ORACLE") ||
	       			targetProductName.toUpperCase().contains("DB2")
	       		) {
	        		targetColumnType = "CLOB";
	           		targetColumnLength = 0;
	    		}
	       		else if (targetProductName.toUpperCase().contains("FIREBIRD")) {
	            	targetColumnType = "BLOB";
	               	targetColumnLength = 0;
	        	}
	       		else if (targetProductName.toUpperCase().contains("ANYWHERE")) {
	                targetColumnType = "TEXT";
	                targetColumnLength = 0;
	       		}
	       		else if (targetProductName.toUpperCase().contains("VERTICA")) {
	       			targetColumnType = "LONG VARCHAR";
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
	           		targetProductName.toUpperCase().contains("H2") ||
	       			targetProductName.toUpperCase().contains("HDB") ||
	           		targetProductName.toUpperCase().contains("HSQL") ||
	           		targetProductName.toUpperCase().contains("TERADATA")
	           	) {
	        		targetColumnType = "CLOB";
	    		}
	       		else if (targetProductName.toUpperCase().contains("FIREBIRD")) {
	            	targetColumnType = "BLOB";
	            	targetColumnTypeAttribute = "";
	       			targetColumnLength = 4000;
	       			//targetColumnLength = 32767;
	        	}
	       		else if (targetProductName.toUpperCase().contains("NETEZZA")) {
	       			targetColumnType = "VARCHAR";
	       			targetColumnLength = 1000;
	    		}
	       		else if (targetProductName.toUpperCase().contains("VERTICA")) {
	           		targetColumnType = "LONG VARCHAR";
	                targetColumnLength = 0;
	           	}
	       		else {
	        		targetColumnType = "XML";
	    		}
	       	}
	   		// BLOB types
	       	else if (
	       		(
		       		sourceColumnType.contains("BLOB") ||
		       		sourceColumnType.contains("BYTE") ||
		       		sourceColumnType.contains("BFILE") ||
		       		sourceColumnType.contains("LONG") ||
		       		sourceColumnType.contains("IMAGE") ||
		       		sourceColumnType.contains("BINARY") ||
		       		sourceColumnType.contains("GEOGRAPHY") ||
		       		sourceColumnType.contains("GEOMETRY") ||
		       		sourceColumnType.contains("ARRAY") ||
		       		sourceColumnType.contains("OTHER") ||
		       		sourceColumnTypeAttribute.contains("FOR BIT DATA")
	       		) &&
	       		!(
	    	       	sourceColumnType.contains("FLOAT") ||
	    	       	sourceColumnType.contains("DOUBLE")
	           	)
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
	       		else if (targetProductName.toUpperCase().contains("NETEZZA")) {
	       			targetColumnType = "VARCHAR";
	       			targetColumnLength = 1000;
	    		}
	       		else if (targetProductName.toUpperCase().contains("ANYWHERE")) {
	        		targetColumnType = "IMAGE";
	    		}
	       		else if (targetProductName.toUpperCase().contains("VERTICA")) {
	        		targetColumnType = "LONG VARBINARY";
	    		}
	       		else if (targetProductName.toUpperCase().contains("DERBY")) {
	        		targetColumnType = "LONG VARCHAR FOR BIT DATA";
	    		}
	       		else if (targetProductName.toUpperCase().contains("FIREBIRD")) {
	            	targetColumnType = "BLOB";
	            	targetColumnTypeAttribute = "";
	       			targetColumnLength = 0;
	    		}
	       		else {
	        		targetColumnType = "BLOB";
	    		}
	       	}
	   		// Date/time types
	   		else if (
	   			sourceColumnType.equalsIgnoreCase("TIME")
	   		) {
	       		if (
	       			targetProductName.toUpperCase().contains("DB2") ||
	       			targetProductName.toUpperCase().contains("DERBY") ||
	       			targetProductName.toUpperCase().contains("FIREBIRD") ||
	       			targetProductName.toUpperCase().contains("H2") ||
	       			targetProductName.toUpperCase().contains("HDB") ||
	       			targetProductName.toUpperCase().contains("HSQL") ||
	       			targetProductName.toUpperCase().contains("MYSQL") ||
	       			targetProductName.toUpperCase().contains("POSTGRE") ||
	       			targetProductName.toUpperCase().contains("NETEZZA")
	       		) {
	               	targetColumnType = "TIME";
	       		}
	       	}
	       	else if (
	       			sourceColumnType.contains("DATE") ||
	       			sourceColumnType.contains("TIME") ||
	       			sourceColumnType.contains("YEAR")
	       	) {
	       		if (
	       			targetProductName.toUpperCase().contains("ORACLE") ||
	       			targetProductName.toUpperCase().contains("DB2")
	       		) {
	           		targetColumnType = "DATE";
	       		}
	       		else if (
	       			targetProductName.toUpperCase().contains("DERBY") ||
	       			targetProductName.toUpperCase().contains("FIREBIRD") ||
	       			targetProductName.toUpperCase().contains("POSTGRE") ||
	       			targetProductName.toUpperCase().contains("TERADATA")
	       		) {
	               	targetColumnType = "TIMESTAMP";
	       		}
	       		else if (targetProductName.toUpperCase().contains("INFORMIX")) {
	           		targetColumnType = "DATETIME YEAR TO FRACTION(3)";
	       		}
	       		else if (targetProductName.toUpperCase().contains("HDB")) {
	               	targetColumnType = "SECONDDATE";
	       		}
	       		else if (targetProductName.toUpperCase().contains("MYSQL")) {
	                targetColumnType = "DATETIME";
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
	   				targetProductName.toUpperCase().contains("TERADATA") ||
	   				targetProductName.toUpperCase().contains("FIREBIRD") ||
	   				targetProductName.toUpperCase().contains("VERTICA")
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
	       			else if (targetProductName.toUpperCase().contains("FIREBIRD") && sourceColumnPrecision > 18) {
	       				targetColumnType = "FLOAT";
	   				}
	       			else if (targetProductName.toUpperCase().contains("MYSQL") && sourceColumnPrecision > 30) {
	       				targetColumnType = "FLOAT";
	   				}
	       			else if (targetProductName.toUpperCase().contains("DB2") && sourceColumnPrecision > 31) {
	       				targetColumnType = "DECFLOAT";
	   				}
	       			else if (targetProductName.toUpperCase().contains("DERBY") && sourceColumnPrecision > 31) {
	       				targetColumnType = "FLOAT";
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
	       			else if (targetProductName.toUpperCase().contains("NETEZZA") && sourceColumnPrecision > 38) {
	       				targetColumnType = "FLOAT";
	   				}
	       			else if (targetProductName.toUpperCase().contains("POSTGRES") && sourceColumnPrecision > 1000) {
	       				targetColumnType = "DOUBLE PRECISION";
	   				}
	       			else if (targetProductName.toUpperCase().contains("ANYWHERE") && sourceColumnPrecision > 127) {
	       				targetColumnType = "DOUBLE";
	   				}
	       			else if (targetProductName.toUpperCase().contains("VERTICA") && sourceColumnPrecision > 1024) {
	       				targetColumnType = "DOUBLE PRECISION";
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
	       			if (targetProductName.toUpperCase().contains("FIREBIRD")) {
	           			targetColumnPrecision = 18;
	       			}
	       			else {
	           			targetColumnPrecision = 22;
	       			}
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
	       		}
	       		else {
	       			targetColumnType = "NUMERIC";
	       		}
	       		//
	   			if (targetProductName.toUpperCase().contains("ORACLE") && sourceColumnPrecision > 38) {
	       			targetColumnType = "FLOAT";
	   			}
	   			else if (targetProductName.toUpperCase().contains("FIREBIRD") && sourceColumnPrecision > 18) {
	   				targetColumnType = "FLOAT";
				}
	   			else if (targetProductName.toUpperCase().contains("MYSQL") && sourceColumnPrecision > 30) {
	   				targetColumnType = "FLOAT";
				}
	   			else if (targetProductName.toUpperCase().contains("DB2") && sourceColumnPrecision > 31) {
	   				targetColumnType = "DECFLOAT";
				}
	   			else if (targetProductName.toUpperCase().contains("DERBY") && sourceColumnPrecision > 31) {
	   				targetColumnType = "FLOAT";
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
	   			else if (targetProductName.toUpperCase().contains("NETEZZA") && sourceColumnPrecision > 38) {
	   				targetColumnType = "FLOAT";
				}
	   			else if (targetProductName.toUpperCase().contains("POSTGRES") && sourceColumnPrecision > 1000) {
	   				targetColumnType = "DOUBLE PRECISION";
				}
	   			else if (targetProductName.toUpperCase().contains("ANYWHERE") && sourceColumnPrecision > 127) {
	   				targetColumnType = "DOUBLE";
				}
	   			else if (targetProductName.toUpperCase().contains("VERTICA") && sourceColumnPrecision > 1024) {
	   				targetColumnType = "DOUBLE PRECISION";
				}
	   			else if (targetProductName.toUpperCase().contains("INFORMIX") && sourceColumnPrecision > 32) {
	       			targetColumnType = "FLOAT";
				}
	   			else {
	   				targetColumnPrecision = sourceColumnPrecision;
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
	   	   			targetProductName.toUpperCase().contains("FIREBIRD") ||
	   	   			targetProductName.toUpperCase().contains("ANYWHERE")
	   	   	   	) {
	   	   	   		targetColumnType = "INT";
	   	   	   		targetColumnPrecision = 0;
	   	   	   		targetColumnScale = 0;
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
	       	else {
	       		targetColumnType = sourceColumnType;
	       		targetColumnLength = sourceColumnLength;
	   			targetColumnPrecision = sourceColumnPrecision;
	   			targetColumnScale = sourceColumnScale;
	       	}
	 		
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
