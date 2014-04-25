package org.openbusinessintelligence.load.bodi;

import org.w3c.dom.*;

public class BodiDataFlowBean {
	//private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(BodiDataFlowBean.class.getPackage().getName());

	//  properties
	private String sourceDataStore = "";
	private String sourceOwnerName = "";
	private String sourceTableName = "";
	private String targetDataStore = "";
	private String targetOwnerName = "";
	private String targetTableName = "";
	private String dataFlowName = "";
	//
	private String[] sourceColumns = null;
	private String[] targetColumns = null;
	private String[] dataTypes = null;
	
	private String[] defaultColumns = null;
	private String[] defaultValues = null;
	private String[] defaultTypes = null;
	
	// Internal objects
	private Element dataFlow;
	private Element transforms;
	//
	private Element databaseTableSource;
	private Element databaseTableTarget;
	private Element sourceOutputView;
	private Element targetInputView;
	//
	private Element schema;
	private Element element;
	private Element query;
	private Element select;
	private Element from;
	private Element tablespec;
	private Element projection;
	private Element expression;
	//
	private Element attributes;
	private Element attribute;
	//
	private Element ldrConfigurations;
	private Element ldrConfiguration;
	//
	String datatype;
	String size;
	String precision;
	String scale;
	
    // Constructor
    public BodiDataFlowBean() {
        super();
    }

    // Set properties methods
    public void setSourceDataStore(String property) {
    	sourceDataStore = property;
    }
    
    public void setSourceOwnerName(String property) {
    	sourceOwnerName = property;
    }
    
    public void setSourceTableName(String property) {
    	sourceTableName = property;
    }
    
    public void setTargetDataStore(String property) {
    	targetDataStore = property;
    }
    
    public void setTargetOwnerName(String property) {
    	targetOwnerName = property;
    }
    
    public void setTargetTableName(String property) {
    	targetTableName = property;
    }
    
    //
    public void setSourceColumns(String[] property) {
    	sourceColumns = property;
    }
    
    public void setTargetColumns(String[] property) {
    	targetColumns = property;
    }
    
    public void setDataTypes(String[] property) {
    	dataTypes = property;
    }
    
    
    public void setDefaultColumns(String[] property) {
    	defaultColumns = property;
    }
    
    public void setDefaultValues(String[] property) {
    	defaultValues = property;
    }
    
    public void setDefaultTypes(String[] property) {
    	defaultTypes = property;
    }
	
    // Dataflow properties
	public void setDataFlowName(String property) {
		dataFlowName = property;
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
       	dataFlow = document.createElement("DIDataflow");
    	dataFlow.setAttribute("name", dataFlowName);
    	document.appendChild(dataFlow);
    	transforms = document.createElement("DITransforms");
    	dataFlow.appendChild(transforms);
    	
    	// source
	    databaseTableSource = document.createElement("DIDatabaseTableSource");
	    databaseTableSource.setAttribute("datastoreName", sourceDataStore);
	    databaseTableSource.setAttribute("ownerName", sourceOwnerName);
	    databaseTableSource.setAttribute("tableName", sourceTableName);
    	transforms.appendChild(databaseTableSource);
    	sourceOutputView = document.createElement("DIOutputView");
    	sourceOutputView.setAttribute("name", sourceTableName);
    	databaseTableSource.appendChild(sourceOutputView);
    	
    	// target
        databaseTableTarget = document.createElement("DIDatabaseTableTarget");
        databaseTableTarget.setAttribute("datastoreName", targetDataStore);
        databaseTableTarget.setAttribute("ownerName", targetOwnerName);
        databaseTableTarget.setAttribute("tableName", targetTableName);
    	transforms.appendChild(databaseTableTarget);
    	targetInputView = document.createElement("DIInputView");
    	targetInputView.setAttribute("name", "Query");
    	databaseTableTarget.appendChild(targetInputView);
    	// target attributes
    	attributes = document.createElement("DIAttributes");
    	databaseTableTarget.appendChild(attributes);
        /*attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","loader_template_table");
        attribute.setAttribute("value","yes");
        attributes.appendChild(attribute);*/
        // enable loader configurations
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","ldr_configuration_enabled");
        attribute.setAttribute("value","yes");
        attributes.appendChild(attribute);
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","ldr_configurations");
        attribute.setAttribute("hasNestedXMLTree","true");
        attributes.appendChild(attribute);
        //loader configuration
        ldrConfigurations = document.createElement("LDRConfigurations");
        attribute.appendChild(ldrConfigurations);
        ldrConfiguration = document.createElement("LDRConfiguration");
        ldrConfiguration.setAttribute("database_type","Oracle");
        ldrConfiguration.setAttribute("database_version","Oracle 11g");
        ldrConfigurations.appendChild(ldrConfiguration);
        element = document.createElement("loader_xact_size");
        element.setTextContent("1000000");
        ldrConfiguration.appendChild(element);
    	
    	// query schema
	   	query = document.createElement("DIQuery");
	    transforms.appendChild(query);
	    schema = document.createElement("DISchema");
	    schema.setAttribute("name", "Query");
	    query.appendChild(schema);
	    
	    for (int i=0; i<defaultColumns.length; i++) {
	    	
	    	element = document.createElement("DIElement");
	    	element.setAttribute("name", defaultColumns[i]);
	    	element = setType(element,defaultTypes[i]);
	    	schema.appendChild(element);
	    }
	    
	    for (int i=0; i<targetColumns.length; i++) {
	    	element = document.createElement("DIElement");
	    	element.setAttribute("name", targetColumns[i]);
	    	element = setType(element,dataTypes[i]);
	    	schema.appendChild(element);
	    }
	    
	    // query select
	    select = document.createElement("DISelect");
	    query.appendChild(select);
	    from = document.createElement("DIFrom");
	    select.appendChild(from);
	    tablespec = document.createElement("DITableSpec");
	    tablespec.setAttribute("name", sourceTableName);
	    from.appendChild(tablespec);
	    projection = document.createElement("DIProjection");
	    select.appendChild(projection);
	    	
	    for (int i=0; i<defaultColumns.length; i++) {
	       	expression = document.createElement("DIExpression");
	       	expression.setAttribute("expr", defaultValues[i]);
	       	expression.setAttribute("isString", "true");
	       	projection.appendChild(expression);
	    }
	    	
	    for (int i=0; i<sourceColumns.length; i++) {
	       	expression = document.createElement("DIExpression");
	       	expression.setAttribute("expr", sourceColumns[i]);
	       	expression.setAttribute("isString", "true");
	       	projection.appendChild(expression);
	    }
    	
    	return dataFlow;
    }
    
}