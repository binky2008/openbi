package org.openbusinessintelligence.load.bodi;

import org.w3c.dom.*;

public class BodiTableBean {
	//private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(BodiTableBean.class.getPackage().getName());

	//  properties
	private String dataStore = "";
	private String ownerName = "";
	private String tableName = "";
	//
	private String[] columns = null;
	private String[] dataTypes = null;
	private String[] defaultColumns = null;
	private String[] defaultTypes = null;
	
	// Internal objects
	private Element table;
	private Element column;
	//
	String datatype;
	String size;
	String precision;
	String scale;
	
    // Constructor
    public BodiTableBean() {
        super();
    }

    // Set properties methods
    public void setDataStore(String property) {
    	dataStore = property;
    }
    
    public void setOwnerName(String property) {
    	ownerName = property;
    }
    
    public void setTableName(String property) {
    	tableName = property;
    }
    
    //
    public void setColumns(String[] property) {
    	columns = property;
    }
    
    public void setDataTypes(String[] property) {
    	dataTypes = property;
    }
    
    public void setDefaultColumns(String[] property) {
    	defaultColumns = property;
    }
    
    public void setDefaultTypes(String[] property) {
    	defaultTypes = property;
    }
    
    private Element setType(Element elm, String typeDef){
    	datatype = typeDef.split("\\(")[0];
    	if (typeDef.split("\\(").length>1) {
        	size = typeDef.split("\\(")[1].split("\\)")[0];
    	}
    	else {
    		size = "";
    	}
    	
    	if (datatype.toUpperCase().contains("CHAR")) {
    		elm.setAttribute("datatype","VARCHAR");
    		elm.setAttribute("size",size);
    	}
    	else if (
    		datatype.toUpperCase().contains("NUM") ||
    		datatype.toUpperCase().contains("INT") ||
    		datatype.toUpperCase().contains("DEC") ||
    		datatype.toUpperCase().contains("BIN")
    	) {
    		if (size.split(",").length<2) {
    			if (size.split(",").length==0 || size.split(",")[0].equals("5")) {
            		elm.setAttribute("datatype","INT");
    			}
    			else {
    				elm.setAttribute("datatype","DECIMAL");
            		elm.setAttribute("precision",size.split(",")[0]);
            		elm.setAttribute("scale","0");
    			}
    		}
    		else {
        		elm.setAttribute("datatype","DECIMAL");
        		elm.setAttribute("precision",size.split(",")[0]);
        		elm.setAttribute("scale",size.split(",")[1]);
    		}
    	}
    	else if (
        	datatype.toUpperCase().contains("DATE") ||
        	datatype.toUpperCase().contains("TIME")
        ) {
    		elm.setAttribute("datatype","DATETIME");
    	}
    	
    	return elm;
    	
    }

    // Creation methods
    public Element getElement(Document document) throws Exception {
    	
    	// root element
    	table = document.createElement("DITable");
    	table.setAttribute("name", tableName);
    	table.setAttribute("owner", ownerName);
    	table.setAttribute("datastore", dataStore);
    	document.appendChild(table);
    	
    	for (int i=0; i<defaultColumns.length; i++) {
    		column = document.createElement("DIColumn");
    		column.setAttribute("name", defaultColumns[i]);
    		column = setType(column,defaultTypes[i]);
    		table.appendChild(column);
    	}
    	
    	for (int i=0; i<columns.length; i++) {
    		
    		column = document.createElement("DIColumn");
    		column.setAttribute("name", columns[i]);
    		column = setType(column,dataTypes[i]);
    		table.appendChild(column);
    	}
		
    	return table;
    }
    
}