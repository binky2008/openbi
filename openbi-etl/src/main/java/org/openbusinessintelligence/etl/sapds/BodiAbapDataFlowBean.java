package org.openbusinessintelligence.etl.sapds;

import org.w3c.dom.*;

public class BodiAbapDataFlowBean {
	//private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(BodiAbapDataFlowBean.class.getPackage().getName());

	//  properties
	private String sourceDataStore = "";
	private String sourceTableName = "";
	private String dataFlowName = "";
	// Internal objects
	private Element dataFlow;
	private Element transforms;
	//
	private Element databaseTableSource;
	private Element databaseTableTarget;
	private Element sourceOutputView;
	private Element targetInputView;
	//
	private Element attributes;
	private Element attribute;
	
    // Constructor
    public BodiAbapDataFlowBean() {
        super();
    }

    // Set properties methods
    public void setSourceDataStore(String property) {
    	sourceDataStore = property;
    }
    
    public void setSourceTableName(String property) {
    	sourceTableName = property;
    }
	
    // Dataflow properties
	public void setDataFlowName(String property) {
		dataFlowName = property;
	}

    // Creation methods
    public Element getElement(Document document) throws Exception {
    	
    	// root element
        dataFlow = document.createElement("DIR3Dataflow");
        attributes = document.createElement("DIAttributes");
    	dataFlow.setAttribute("name", dataFlowName);
    	document.appendChild(dataFlow);

    	attributes = document.createElement("DIAttributes");
        dataFlow.appendChild(attributes);
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","abap_datastore");
        attribute.setAttribute("value",sourceDataStore);
        attributes.appendChild(attribute);
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","abap_program_file");
        attribute.setAttribute("value","Z_" + sourceTableName + ".abap");
        attributes.appendChild(attribute);
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","abap_program_name_in_r3");
        attribute.setAttribute("value","Z_" + sourceTableName);
        attributes.appendChild(attribute);
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","job_name");
        attribute.setAttribute("value","Z_" + sourceTableName);
        attributes.appendChild(attribute);
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","cache");
        attribute.setAttribute("value","yes");
        attributes.appendChild(attribute);
    	
    	transforms = document.createElement("DITransforms");
    	dataFlow.appendChild(transforms);
    	
	    databaseTableSource = document.createElement("DIDatabaseTableSource");
	    databaseTableSource.setAttribute("datastoreName", sourceDataStore);
	    databaseTableSource.setAttribute("tableName", sourceTableName);
	    
    	transforms.appendChild(databaseTableSource);
    	sourceOutputView = document.createElement("DIOutputView");
    	sourceOutputView.setAttribute("name", sourceTableName);
    	databaseTableSource.appendChild(sourceOutputView);

    	databaseTableTarget = document.createElement("DITempFileTarget");
        databaseTableTarget.setAttribute("filename", sourceTableName + ".dat");
    	transforms.appendChild(databaseTableTarget);
    	targetInputView = document.createElement("DIInputView");
    	targetInputView.setAttribute("name", sourceTableName);
    	databaseTableTarget.appendChild(targetInputView);
    	
    	return dataFlow;
    }
    
}