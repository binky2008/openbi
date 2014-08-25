package org.openbusinessintelligence.core.db;

import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class TypeConversionBean {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(TypeConversionBean.class);

	org.w3c.dom.Document typeConvertionMatrix = null;
	org.w3c.dom.Document typeOptionMatrix = null;
	
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
    private NodeList nMatrixSourceTypeList;
    private NodeList nMatrixSourceSubTypeList;
    private NodeList nMatrixTargetProductList;
    private NodeList nMatrixTargetTypeList;
    private NodeList nMatrixTargetSubTypeList;
    private Node nNode;
    private Element eMatrixSourceTypeElement;
    private Element eMatrixTargetTypeElement;
    private Element eMatrixProductElement;
    private Element eMatrixSubTypeElement;
    
    private String matrixMatchedTypeID = "";
    private String matrixMatchedTypeName = "";

    private String matrixSourceProductType = "";
    private String matrixTargetProductType = "";
    private String matrixTargetTypeAttribute = "";
    private String matrixTargetOversizedProductType = "";
    private int matrixTargetDataLength = 0;
    private int matrixTargetMaxLength = 0;
    private int matrixTargetMaxScale = 0;
    
    private boolean matrixSourceTypeMatch = false;
    private boolean matrixTargetProductTypeMatch = false;
    private boolean matrixIsOversized = true;

    private String matrixTargetDefaultType = "";
    private int matrixTargetDefaultDataLength = 0;

    private boolean matrixProductLengthOptionMatch = false;
    private boolean matrixProductScaleOptionMatch = false;
    private boolean matrixLengthOption = true;
    private boolean matrixScaleOption = true;
    private boolean matrixDefaultLengthOption = true;
    private boolean matrixDefaultScaleOption = false;
    
    // Setter methods
    public void setTypeConvertionMatrix(org.w3c.dom.Document property) {
    	typeConvertionMatrix = property;
    }
    public void setTypeOptionMatrix(org.w3c.dom.Document property) {
    	typeOptionMatrix = property;
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

    	targetColumnType = "";
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
       			sourceColumnType.toUpperCase().contains("BOOL") ||
       			sourceColumnType.toUpperCase().contains("TINY") ||
       			sourceColumnType.toUpperCase().contains("SMALL") ||
       			sourceColumnType.toUpperCase().contains("MEDIUM") ||
       			sourceColumnType.toUpperCase().contains("BIG") ||
       			sourceColumnType.toUpperCase().contains("LONG") ||
       			sourceColumnType.toUpperCase().contains("SERIAL") ||
       			sourceColumnType.toUpperCase().contains("REAL") ||
       			sourceColumnType.toUpperCase().contains("FLOAT") ||
       			sourceColumnType.toUpperCase().contains("DOUBLE") ||
       			sourceColumnType.toUpperCase().contains("UNSIGNED") ||
       			sourceColumnType.toUpperCase().contains("MONEY") ||
       			sourceColumnType.toUpperCase().contains("TEXT") ||
       			sourceColumnType.toUpperCase().contains("DATE") ||
       			sourceColumnType.toUpperCase().contains("TIME") ||
       			sourceColumnType.toUpperCase().contains("INTERVAL") ||
       			sourceColumnType.toUpperCase().contains("YEAR") ||
       			sourceColumnType.toUpperCase().contains("CLOB") ||
       			sourceColumnType.toUpperCase().contains("BLOB") ||
       			sourceColumnType.toUpperCase().contains("XML") ||
       			sourceColumnType.toUpperCase().contains("JSON") ||
           		// PostgreSQL types
       			sourceColumnType.toUpperCase().contains("INT2") ||
       			sourceColumnType.toUpperCase().contains("INT4") ||
       			sourceColumnType.toUpperCase().contains("INT8") ||
       			sourceColumnType.toUpperCase().contains("FLOAT4") ||
       			sourceColumnType.toUpperCase().contains("FLOAT8") ||
       			sourceColumnType.toUpperCase().contains("BYTEA") ||
       			sourceColumnType.toUpperCase().contains("TXID") ||
       			sourceColumnType.toUpperCase().contains("UUID") ||
       			sourceColumnType.toUpperCase().contains("CIDR") ||
       			sourceColumnType.toUpperCase().contains("INET") ||
       			sourceColumnType.toUpperCase().contains("MACADDR") ||
       			sourceColumnType.toUpperCase().contains("TSQUERY") ||
       			sourceColumnType.toUpperCase().contains("TSVECTOR") ||
       			sourceColumnType.toUpperCase().contains("BOX") ||
       			sourceColumnType.toUpperCase().contains("CIRCLE") ||
       			sourceColumnType.toUpperCase().contains("LINE") ||
           		sourceColumnType.toUpperCase().contains("LSEG") ||
           		sourceColumnType.toUpperCase().contains("PATH") ||
           		sourceColumnType.toUpperCase().contains("POINT") ||
           		sourceColumnType.toUpperCase().contains("POLYGON") ||
    	   		// Oracle specific types
           		sourceColumnType.toUpperCase().contains("SDO_GEOMETRY") ||
           		sourceColumnType.toUpperCase().contains("SDO_RASTER") ||
           		sourceColumnType.toUpperCase().contains("BFILE") ||
           		sourceColumnType.toUpperCase().contains("ROWID") ||
           		// SQL Server specific types
           		sourceColumnType.toUpperCase().contains("IMAGE") ||
           		sourceColumnType.toUpperCase().contains("GEOGRAPHY") ||
           		sourceColumnType.toUpperCase().contains("GEOMETRY") ||
               	sourceColumnType.toUpperCase().contains("HIERARCHYID") ||
               	sourceColumnType.toUpperCase().contains("UNIQUEIDENTIFIER") ||
           		// Informix specific types
               	sourceColumnType.toUpperCase().contains("BINARY18") ||
               	sourceColumnType.toUpperCase().contains("BINARYVAR")
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
       		else if (sourceColumnType.toUpperCase().contains("OTHER")) {
       			targetColumnType = "OTHER";
       		}
       		else if (
       			sourceColumnType.toUpperCase().contains("ALPHANUM") &&
           		targetProductName.toUpperCase().contains("HDB")
       		) {
                targetColumnPrecision = 0;
                targetColumnScale = 0;
                targetColumnLength = sourceColumnLength;
       		}
       		else if (sourceColumnType.toUpperCase().contains("BIT")) {
               	if (
               		targetProductName.toUpperCase().contains("SQL SERVER") ||
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
      		else if (sourceColumnType.toUpperCase().contains("INT")) {
            	if (
            		targetProductName.toUpperCase().contains("DERBY") ||
            		targetProductName.toUpperCase().contains("FIREBIRD") ||
            		targetProductName.toUpperCase().contains("HDB") ||
            		targetProductName.toUpperCase().contains("HSQL") ||
            		targetProductName.toUpperCase().contains("INFORMIX") ||
            		targetProductName.toUpperCase().contains("NETEZZA") ||
            		targetProductName.toUpperCase().contains("SQL SERVER") ||
            		targetProductName.toUpperCase().contains("TERADATA") ||
            		targetProductName.toUpperCase().contains("VERTICA")
            	) {
                    targetColumnLength = 0;
                }
                else {
                    targetColumnLength = sourceColumnLength;
                }
           	}
       		else if (
       			sourceColumnType.toUpperCase().contains("CHAR") ||
       			sourceColumnType.toUpperCase().contains("BINAR")
            ) {
                if (
                    targetProductName.toUpperCase().contains("SQL SERVER") &&
                    sourceColumnLength > 8000
                ) {
                    targetColumnLength = -1;
                }
                else {
                   	targetColumnLength = sourceColumnLength;
                }
       		}
       		else if (sourceColumnType.toUpperCase().contains("GRAPHIC")) {
       			targetColumnLength = sourceColumnLength;
           	}
       		else if (
       			targetProductName.toUpperCase().contains("ORACLE") &&
       			sourceColumnType.contains("NUMBER") &&
       			sourceColumnPrecision > 38
       		) {
       			targetColumnType = "DOUBLE";
       		}
       		// Informix special types
       		else if (
               	sourceColumnType.toUpperCase().contains("BYTE") &&
                targetProductName.toUpperCase().contains("INFORMIX")
            ) {
                targetColumnPrecision = 0;
                targetColumnScale = 0;
                targetColumnLength = 0;
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
       		// Teradata special types
            else if (sourceColumnType.equalsIgnoreCase("PERIOD")) {
    			targetColumnLength = 0;
    			targetColumnPrecision = 0;
    			targetColumnScale = 0;
            }
            else if (sourceColumnType.equalsIgnoreCase("N")) {
            	targetColumnType = "DECIMAL";
           		targetColumnLength = 38;
	   			targetColumnPrecision = 38;
	   			targetColumnScale = sourceColumnScale;
            }
            else if (
            	sourceColumnType.toUpperCase().contains("BYTE") &&
                targetProductName.toUpperCase().contains("TERADATA")
            ) {
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
   	        
   			// Get target db type
   			if (sourceColumnTypeAttribute.equalsIgnoreCase("FOR BIT DATA")) {
   				matrixSourceProductType = sourceColumnType + " " + sourceColumnTypeAttribute;
   			}
   			else {
   				matrixSourceProductType = sourceColumnType;
   			}
   			logger.debug("SOURCE TYPE NAME = " + matrixSourceProductType);
   			logger.debug("SOURCE JDBC TYPE = " + columnJdbcType);
   	        matrixMatchedTypeName = "";
   	        matrixMatchedTypeID = "";
   	        matrixTargetProductType = "";
   	        matrixTargetTypeAttribute = "";
   	        matrixTargetDefaultType = "";
   	        matrixTargetOversizedProductType = "";
   	        matrixTargetMaxLength = 0;
   	        matrixTargetMaxScale = 0;
   	        matrixTargetDataLength = 0;
   	        matrixTargetDefaultDataLength = 0;
   	        
   			nMatrixSourceTypeList = typeConvertionMatrix.getElementsByTagName("jdbcType");
   			
   			// Scroll source types in the matrix
   	 		for (int t = 0; t < nMatrixSourceTypeList.getLength(); t++) {
   	 			nNode = nMatrixSourceTypeList.item(t);
   	 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	 				eMatrixSourceTypeElement = (Element) nNode;

   	 				// Match by type name
   	 				nMatrixSourceSubTypeList = eMatrixSourceTypeElement.getChildNodes();
   	 				for (int s = 0; s < nMatrixSourceSubTypeList.getLength(); s++) {
   	     				if (
   	         				nMatrixSourceSubTypeList.item(s).getNodeName().equals("typeName") &&
   	     					nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(matrixSourceProductType))
   	     				) {
   	     					// Jdbc type match by name
   	     					matrixMatchedTypeName = nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue();
   	     					logger.debug("MATCHED BY SOURCE TYPE NAME = " + matrixMatchedTypeName);
   	     				}
   	     				else if (
   	             			nMatrixSourceSubTypeList.item(s).getNodeName().equals("typeId") &&
   	         				nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(columnJdbcType))
   	         			) {
   	         				// Jdbc type match by id
   	     					matrixMatchedTypeID = nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue();
   	         				logger.debug("MATCHED BY JDBC TYPE ID = " + matrixMatchedTypeID);
   	     				}
   	 				}
   	 			}
   	 		}
   	 		
   	 		for (int t = 0; t < nMatrixSourceTypeList.getLength(); t++) {
   	 			matrixSourceTypeMatch = false;
   	 			nNode = nMatrixSourceTypeList.item(t);
   	 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	 				eMatrixSourceTypeElement = (Element) nNode;
   	     			nMatrixSourceSubTypeList = eMatrixSourceTypeElement.getChildNodes();
   	     			for (int s = 0; s < nMatrixSourceSubTypeList.getLength(); s++) {
   	         			if (
   	             			nMatrixSourceSubTypeList.item(s).getNodeName().equals("typeName") &&
   	         				nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(matrixSourceProductType))
   	         			) {
   	         				// Jdbc type match by name
   	         				matrixSourceTypeMatch = true;
   	         				logger.debug("FINALLY SOURCE MATCH BY NAME = " + nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue());
   	         			}
   	         			else if (
   	         				matrixMatchedTypeName.equals("") &&
   	                   		nMatrixSourceSubTypeList.item(s).getNodeName().equals("typeId") &&
   	                		nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(columnJdbcType))
   	                	) {
   	    	        		// Jdbc type match by id
   	         				matrixSourceTypeMatch = true;
   	    	        		logger.debug("FINALLY SOURCE MATCH BY ID = " + nMatrixSourceSubTypeList.item(s).getChildNodes().item(0).getNodeValue());
   	             		}
   	     			}
   	 				
   	 				if (matrixSourceTypeMatch) {
   	   	     			
   	 					matrixTargetProductTypeMatch = false;
   	 					nMatrixTargetProductList = eMatrixSourceTypeElement.getElementsByTagName("dbType");
   	 					
   	 		    		// Scroll db products in the matrix
   	 		     		for (int p = 0; p < nMatrixTargetProductList.getLength(); p++) {
   	 		     			nNode = nMatrixTargetProductList.item(p);
   	 		     			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	 		     				eMatrixProductElement = (Element) nNode;
   	 		     				if (targetProductName.toUpperCase().contains(eMatrixProductElement.getElementsByTagName("productName").item(0).getChildNodes().item(0).getNodeValue())) {
   	 		     					// Product name match
   	 		     					matrixIsOversized = false;
   	 		     					logger.debug("PRODUCT MATCH");
   	 		     					
   	 		     					// Search for the type name
   	 		     					nMatrixTargetTypeList = eMatrixProductElement.getChildNodes();
   	 		     					//logger.debug("NO. OF TYPES = " + nTargetTypeList.getLength());
   	 		        				for (int tt = 0; tt < nMatrixTargetTypeList.getLength(); tt++) {
   	     		     					if (nMatrixTargetTypeList.item(tt).getNodeName().equals("typeName")) {
   	     		     						matrixTargetProductTypeMatch = true;
   	            		     				matrixTargetProductType = nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue();
   	         		     					//logger.debug("PRODUCT TARGET TYPE = " + targetProductType);
   	     		     					}
   	     		     					else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("typeAttribute")) {
   	     		     						matrixTargetTypeAttribute = nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue();
   	     		     						logger.debug("PRODUCT TARGET TYPE ATTRIBUTE = " + matrixTargetTypeAttribute);
   	     		     					}
   	     		     					else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("dataLength")) {
   	     		     						matrixTargetDataLength = Integer.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
   	     		     						//logger.debug("PRODUCT TARGET DATA LENGTH = " + targetMaxLength);
   	     		     					}
   	     		     					else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("maxLength")) {
   	     		     						matrixTargetMaxLength = Integer.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
   	     		     						//logger.debug("PRODUCT MAX LENGTH = " + targetMaxLength);
   	   	     		     					if (matrixTargetMaxLength > 0 && matrixTargetMaxLength < sourceColumnLength) {
   	   	     		     						matrixIsOversized = true;
   	   	     		     						//logger.debug("IS OVERSIZED = " + isOversized);
   	   	     		     					}
   	     		     					}
   	 		        				}
   	 		     					
   	 		     					// Search for sub type
   	 		     					nMatrixTargetSubTypeList = eMatrixProductElement.getElementsByTagName("SubType");
   	 		     					//logger.debug("NO. OF SUBTYPES = " + nTargetSubTypeList.getLength());
   	 		     		     		for (int st = 0; st < nMatrixTargetSubTypeList.getLength(); st++) {
   	 		     		     			nNode = nMatrixTargetSubTypeList.item(st);
   	     		     		     		if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	     	     		     				eMatrixSubTypeElement = (Element) nNode;
	             		     				//logger.debug("MAX LENGTH = " + eSubTypeElement.getElementsByTagName("maxLength").item(0).getChildNodes().item(0).getNodeValue());
   	         		     					if (
   	         		     						!matrixTargetProductTypeMatch &&
   	         		     						Integer.valueOf(eMatrixSubTypeElement.getElementsByTagName("maxLength").item(0).getChildNodes().item(0).getNodeValue()) >= sourceColumnLength
   	         		     					) {
   	         		     						matrixTargetProductTypeMatch = true;
   	         		     						matrixIsOversized = false;
   	         		     						nMatrixTargetTypeList = eMatrixSubTypeElement.getChildNodes();
	   	         		     					for (int tt = 0; tt < nMatrixTargetTypeList.getLength(); tt++) {
	   	    	     		     					if (nMatrixTargetTypeList.item(tt).getNodeName().equals("typeName")) {
	   	    	     		     						matrixTargetProductType = nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue();
	   	   	             		     					//logger.debug("PRODUCT TARGET TYPE = " + targetProductType);
	   	    	     		     					}
	   	         		     					}
   	     		     		     			}
   	         		     					else if (!matrixTargetProductTypeMatch){
   	         		     						matrixIsOversized = true;
   	         		     					}
   	     		     		     		}
   	 		     		     		}
  	     		     				logger.debug("IS OVERSIZED = " + matrixIsOversized);
   	 		     		     		
   	 		     		     		// If size bigger than allowed max
   	 		     		     		if (matrixIsOversized) {
   	 		     		     			matrixLengthOption = false;
   	   	 		     					// Search for the oversize properties
   	   	 		        				for (int tt = 0; tt < nMatrixTargetTypeList.getLength(); tt++) {
   	   	     		     					if (nMatrixTargetTypeList.item(tt).getNodeName().equals("oversizeTypeName")) {
   	   	     		     						matrixTargetOversizedProductType = eMatrixProductElement.getElementsByTagName("oversizeTypeName").item(0).getChildNodes().item(0).getNodeValue();
   	   	         		     					logger.debug("OVERSIZE TARGET TYPE = " + matrixTargetOversizedProductType);
   	   	         		     					matrixTargetProductTypeMatch = true;
   	   	     		     					}
   	   	     		     					else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("oversizeDataLength")) {
   	   	     		     						matrixTargetDataLength = Integer.valueOf(eMatrixProductElement.getElementsByTagName("oversizeDataLength").item(0).getChildNodes().item(0).getNodeValue());
   	   	         		     					logger.debug("OVERSIZE DATA LENGTH = " + matrixTargetDataLength);
   	   	     		     					}
   	   	 		        				}
   	   	 		        				if (matrixTargetOversizedProductType.equals("")) {
   	   	 		        					matrixTargetDataLength = matrixTargetMaxLength;
   	   	 		        				}
   	   	 		        				else {
   	   	 		        					matrixTargetProductType = matrixTargetOversizedProductType;
   	   	 		        					matrixTargetTypeAttribute = "";
   	   	 		        				}
   	 		     		     		}
   	 		     				}
   	 		     				else if (eMatrixProductElement.getElementsByTagName("productName").item(0).getChildNodes().item(0).getNodeValue().equalsIgnoreCase("DEFAULT")) {
   	 		     					// Get defaults
   	 		     					nMatrixTargetTypeList = eMatrixProductElement.getChildNodes();
   	 		        				for (int tt = 0; tt < nMatrixTargetTypeList.getLength(); tt++) {

   	     		     					if (nMatrixTargetTypeList.item(tt).getNodeName().equals("typeName")) {
   	     		     						matrixTargetDefaultType = nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue();
   	   	 		        					logger.debug("DEFAULT TARGET TYPE = " + matrixTargetDefaultType);
   	     		     					}
   	     		     					else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("dataLength")) {
   	     		     						matrixTargetDefaultDataLength = Integer.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
   	     		     						logger.debug("DEFAULT TARGET DATA LENGTH = " + matrixTargetMaxLength);
   	   	 		        				}
   	     		     				}
   	 		     				}
   	 		     			}
   	 		     		}
   	 				}
   	 			}
   			}
   	 		
   	 		if (matrixTargetProductTypeMatch) {
   	 			targetColumnType = matrixTargetProductType;
   	 		}
   	 		else {
   	 			targetColumnType = matrixTargetDefaultType;
   	 		}
   	 		
   	 		if (!matrixTargetTypeAttribute.equals("")) {
   	 			targetColumnTypeAttribute = matrixTargetTypeAttribute;
   	 		}
   	 		
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
		        	targetProductName.toUpperCase().contains("DB2") ||
		        	targetProductName.toUpperCase().contains("DERBY") ||
		        	targetProductName.toUpperCase().contains("HDB") ||
		        	targetProductName.toUpperCase().contains("HSQL") ||
	        		targetProductName.toUpperCase().contains("ORACLE") ||
	       			targetProductName.toUpperCase().contains("TERADATA")
	        	) {
	        		targetColumnType = "CLOB";
	        	}
	        	else if (targetProductName.toUpperCase().contains("FIREBIRD")) {
		        	targetColumnType = "BLOB";
		        }
	        	else if (
	        		targetProductName.toUpperCase().contains("NETEZZA") ||
		       		targetProductName.toUpperCase().contains("VERTICA")
		       	) {
		        	targetColumnType = "VARCHAR";
		        }
	        	else {
	        		targetColumnType = "TEXT";
	        	}
	        }
	   		// SQL Server special types
	        else if (
	        	sourceProductName.toUpperCase().contains("SQL SERVER") &&
		   		sourceColumnType.toUpperCase().contains("TIMESTAMP")
	   		) {
	       		if (targetProductName.toUpperCase().contains("POSTGRES")) {
	        		targetColumnType = "BYTEA";
	    		}
	       		else if (targetProductName.toUpperCase().contains("INFORMIX")) {
	        		targetColumnType = "BYTE";
	    		}
	       		else if (targetProductName.toUpperCase().contains("SQL ANYWHERE")) {
	        		targetColumnType = "LONG BINARY";
	    		}
	       		else if (targetProductName.toUpperCase().contains("SQL SERVER")) {
	        		targetColumnType = "ROWVERSION";
	    		}
	       		else if (targetProductName.toUpperCase().contains("VERTICA")) {
	        		targetColumnType = "LONG VARBINARY";
	    		}
	       		else {
	        		targetColumnType = "BLOB";
	    		}
	        }
	        else if (
		   		sourceColumnType.toUpperCase().contains("HIERARCHYID") ||
		   		sourceColumnType.toUpperCase().contains("UNIQUEIDENTIFIER") ||
		   		sourceColumnType.toUpperCase().contains("GEOGRAPHY") ||
		   		sourceColumnType.toUpperCase().contains("GEOMETRY")
	   		) {
	        	if (targetProductName.toUpperCase().contains("ORACLE")) {
	        		targetColumnType = "VARCHAR2";
	        	}
	        	else {
	        		targetColumnType = "VARCHAR";
	        	}
				targetColumnLength = 255;
	        }
   	 		
   	 		logger.debug("TARGET TYPE = " + targetColumnType);
   	        
   			// Get target length
   	 		matrixMatchedTypeName = "";
   	 		matrixTargetProductType = "";

   	 		matrixProductLengthOptionMatch = false;
   	 		matrixProductScaleOptionMatch = false;
   	 		matrixLengthOption = true;
        	matrixScaleOption = false;
        	matrixDefaultLengthOption = true;
        	matrixDefaultScaleOption = false;
	        nMatrixTargetTypeList = typeOptionMatrix.getElementsByTagName("dbType");
	        //logger.debug("NO. OF TYPES: " + nTargetTypeList.getLength());
   	 					
   			// Scroll target types in the matrix
   	 		for (int t = 0; t < nMatrixTargetTypeList.getLength(); t++) {
   	 			matrixSourceTypeMatch = false;
   	 			nNode = nMatrixTargetTypeList.item(t);
   	 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
   	 				eMatrixTargetTypeElement = (Element) nNode;
   	     			nMatrixTargetSubTypeList = eMatrixTargetTypeElement.getChildNodes();
   	     			//logger.debug("NO. OF CHILDS: " + nTargetSubTypeList.getLength());
   	     			for (int s = 0; s < nMatrixTargetSubTypeList.getLength(); s++) {
   	         			if (
   	         				nMatrixTargetSubTypeList.item(s).getNodeName().equals("typeName") &&
   	         				nMatrixTargetSubTypeList.item(s).getChildNodes().item(0).getNodeValue().equalsIgnoreCase(String.valueOf(targetColumnType))
   	         			) {
   	         				// Jdbc type match by name
   	         				matrixSourceTypeMatch = true;
   	         				logger.debug("SOURCE MATCH BY NAME = " + nMatrixTargetSubTypeList.item(s).getChildNodes().item(0).getNodeValue());
   	         			}
   	     			}
	 				
   	     			if (matrixSourceTypeMatch) {
   	   	     			
   	   	     			logger.debug("SOURCE LENGTH = " + sourceColumnLength);
   	   	     			matrixTargetProductTypeMatch = false;
   	 					nMatrixTargetProductList = eMatrixTargetTypeElement.getElementsByTagName("option");
	   	 				
			   	 		for (int p = 0; p < nMatrixTargetProductList.getLength(); p++) {
			   	 			nNode = nMatrixTargetProductList.item(p);
			   	 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
			   	 				eMatrixProductElement = (Element) nNode;
			   	 		     	if (targetProductName.toUpperCase().contains(eMatrixProductElement.getElementsByTagName("productName").item(0).getChildNodes().item(0).getNodeValue())) {
			   	 		     		// Product name match
			   	 		     		logger.debug("PRODUCT MATCH");
			   	 		     		
			   	 		     		// Search for the type name
			   	 		     		nMatrixTargetTypeList = eMatrixProductElement.getChildNodes();
			   	 		     		//logger.debug("NO. OF TYPES = " + nTargetTypeList.getLength());
			   	 		        	for (int tt = 0; tt < nMatrixTargetTypeList.getLength(); tt++) {
			   	     		     		if (nMatrixTargetTypeList.item(tt).getNodeName().equals("lengthOption")) {
			   	     		     			matrixProductLengthOptionMatch = true;
			   	     		     			matrixLengthOption = Boolean.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
			   	         		     		logger.debug("PRODUCT TYPE LENGTH OPTION = " + matrixLengthOption);
			   	     		     		}
			   	     		     		else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("scaleOption")) {
			   	     		     			matrixProductScaleOptionMatch = true;
			   	            		    	matrixScaleOption = Boolean.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
			   	         		     		logger.debug("PRODUCT TYPE SCALE OPTION = " + matrixScaleOption);
			   	     		     		}
			   	     		     		else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("maxDataLength")) {
			   	     		     			matrixTargetMaxLength = Integer.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
			   	         		     		logger.debug("PRODUCT MAX DATA LENGTH = " + matrixTargetMaxLength);
			   	     		     		}
			   	     		     		else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("maxDataScale")) {
			   	     		     			matrixTargetMaxScale = Integer.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
			   	         		     		logger.debug("PRODUCT MAX DATA SCALE = " + matrixTargetMaxScale);
			   	     		     		}
			   	 		        	}
			   	 		     	}
			   	 		     	else if (eMatrixProductElement.getElementsByTagName("productName").item(0).getChildNodes().item(0).getNodeValue().equalsIgnoreCase("DEFAULT")) {
			   	 		     		// Product name match
			   	 		     		nMatrixTargetTypeList = eMatrixProductElement.getChildNodes();
			   	 		     		//logger.debug("NO. OF TYPES = " + nTargetTypeList.getLength());
			   	 		        	for (int tt = 0; tt < nMatrixTargetTypeList.getLength(); tt++) {
			   	     		     		if (nMatrixTargetTypeList.item(tt).getNodeName().equals("lengthOption")) {
			   	     		     			matrixDefaultLengthOption = Boolean.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
			   	         		     		logger.debug("DEFAULT TYPE LENGTH OPTION = " + matrixDefaultLengthOption);
			   	     		     		}
			   	     		     		else if (nMatrixTargetTypeList.item(tt).getNodeName().equals("scaleOption")) {
			   	     		     			matrixDefaultScaleOption = Boolean.valueOf(nMatrixTargetTypeList.item(tt).getChildNodes().item(0).getNodeValue());
			   	         		     		logger.debug("DEFAULT TYPE SCALE OPTION = " + matrixDefaultScaleOption);
			   	     		     		}
			   	 		        	}
			   	 				}
			   	 		    }
			   	 		}
	   	 			}
		 		}
   	 		}
   	 		
   	 		// Set default options for length and scale if product specific ones are missing
   	 		if (!matrixProductLengthOptionMatch) {
   	 			matrixLengthOption = matrixDefaultLengthOption;
   	 		}
   	 		
   	 		if (!matrixProductScaleOptionMatch) {
   	 			matrixScaleOption = matrixDefaultScaleOption;
   	 		}

   	 		// Set length and scale
   	 		if (matrixLengthOption && matrixScaleOption) {
   	 			if (
   	   	 			(
   	   	 				sourceColumnLength == 0 ||
   	   	 				sourceColumnLength > matrixTargetMaxLength ||
   	   	 				sourceColumnPrecision > matrixTargetMaxLength ||
   	   	 				matrixTargetDataLength > matrixTargetMaxLength
   	   	 			) &&
   	   	 			matrixTargetMaxLength > 0
   	   	 		) {
   	   	 			targetColumnLength = matrixTargetMaxLength;
   					targetColumnPrecision = matrixTargetMaxLength;
   					if (sourceColumnScale > targetColumnPrecision) {
   						targetColumnScale = targetColumnPrecision;
   					}
   					else {
   	   					targetColumnScale = sourceColumnScale;
   					}
   	   	 		}
   	 			else {
   					targetColumnLength = sourceColumnLength;
   					targetColumnPrecision = sourceColumnPrecision;
   					if (sourceColumnScale > targetColumnPrecision) {
   						targetColumnScale = targetColumnPrecision;
   					}
   					else {
   	   					targetColumnScale = sourceColumnScale;
   					}
   	 			}
   	 			if (
   	 				matrixTargetMaxScale > 0 &&
   	 				targetColumnScale > matrixTargetMaxScale
   	 			) {
   	 				targetColumnScale = matrixTargetMaxScale;
   	 			}
   	 		}
   	 		else if (matrixLengthOption && !matrixScaleOption) {
   	 			if (matrixTargetDataLength < 0) {
   					targetColumnLength = 0;
   					targetColumnType += "(MAX)";
   	 			}
   	 			else if (
   	   	 			(
   	   	 				sourceColumnLength == 0 ||
   	   	 				sourceColumnLength > matrixTargetMaxLength ||
   	   	 				matrixTargetDataLength > matrixTargetMaxLength
   	   	 			) &&
   	   	 			matrixTargetMaxLength > 0
   	   	 		) {
   	   	 			targetColumnLength = matrixTargetMaxLength;
   	   	 		}
   	 			else if (matrixTargetDataLength > 0) {
   					targetColumnLength = matrixTargetDataLength;
   	 			}
   	 			else if (matrixTargetDefaultDataLength > 0) {
   					targetColumnLength = matrixTargetDefaultDataLength;
   	 			}
   	 			else {
   					targetColumnLength = sourceColumnLength;
   	 			}
				targetColumnPrecision = 0;
				targetColumnScale = 0;
   	 		}
   	 		else {
				targetColumnLength = 0;
				targetColumnPrecision = 0;
				targetColumnScale = 0;
   	 		}
   	 		
   	 		logger.debug("LENGTH OPTION = " + matrixLengthOption);
   	 		logger.debug("SCALE OPTION = " + matrixScaleOption);
	        // NCHAR and NVARCHAR types
	   		/*else if (sourceColumnType.contains("NCHAR") && sourceColumnLength == 1) {
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
	       	else if (
	       		sourceColumnType.contains("DOUBLE") ||
	       		sourceColumnType.contains("REAL")
	       	) {
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
	       	}*/
	 		
		}
   		
    	// Column definition
   		targetColumnDefinition = "";
    	if (targetColumnType.equals("")) {
    		logger.debug("Type conhversion not supported");
    	}
    	else {
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
}
