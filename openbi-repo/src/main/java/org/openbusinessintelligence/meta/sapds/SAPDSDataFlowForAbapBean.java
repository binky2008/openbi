package org.openbusinessintelligence.meta.sapds;

import org.w3c.dom.*;

public class SAPDSDataFlowForAbapBean {
	//private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(BodiDataFlowForAbapBean.class.getPackage().getName());

	// Properties
	private String targetDataStore = "";
	private String targetOwnerName = "";
	private String targetTableName = "";
	private String dataFlowName = "";
	private String abapDataFlowName = "";
	
	// Internal objects
	private Element dataFlow;
	private Element transforms;
	//
	private Element abapFlowSource;
	private Element databaseTableTarget;
	private Element sourceOutputView;
	private Element targetInputView;
	//
	private Element attributes;
	private Element attribute;
	//
	private Element ldrConfigurations;
	private Element ldrConfiguration;
	private Element element;
	
    // Constructor
    public SAPDSDataFlowForAbapBean() {
        super();
    }

    // Set properties methods       
    public void setTargetDataStore(String property) {
    	targetDataStore = property;
    }
    
    public void setTargetOwnerName(String property) {
    	targetOwnerName = property;
    }
    
    public void setTargetTableName(String property) {
    	targetTableName = property;
    }
	
    // Dataflow properties
	public void setDataFlowName(String property) {
		dataFlowName = property;
	}

	public void setAbapDataFlowName(String property) {
		abapDataFlowName = property;
	}

    // Creation methods
    public Element getElement(Document document) throws Exception {
    	
    	// root element
        dataFlow = document.createElement("DIDataflow");
    	dataFlow.setAttribute("name", dataFlowName);
    	document.appendChild(dataFlow);
    	
    	// transforms
    	transforms = document.createElement("DITransforms");
    	dataFlow.appendChild(transforms);
    	
    	// source
    	abapFlowSource = document.createElement("DIR3DataflowCall");
    	abapFlowSource.setAttribute("name", abapDataFlowName);
    	transforms.appendChild(abapFlowSource);
    	sourceOutputView = document.createElement("DIOutputView");
    	sourceOutputView.setAttribute("name", abapDataFlowName);
    	abapFlowSource.appendChild(sourceOutputView);
    	
    	// target
        databaseTableTarget = document.createElement("DIDatabaseTableTarget");
        databaseTableTarget.setAttribute("datastoreName", targetDataStore);
        databaseTableTarget.setAttribute("ownerName", targetOwnerName);
        databaseTableTarget.setAttribute("tableName", targetTableName);
    	transforms.appendChild(databaseTableTarget);
    	targetInputView = document.createElement("DIInputView");
    	targetInputView.setAttribute("name", abapDataFlowName);
    	databaseTableTarget.appendChild(targetInputView);
    	// target attributes
    	attributes = document.createElement("DIAttributes");
    	databaseTableTarget.appendChild(attributes);
        attribute = document.createElement("DIAttribute");
        attribute.setAttribute("name","loader_template_table");
        attribute.setAttribute("value","yes");
        attributes.appendChild(attribute);
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
        /*element = document.createElement("loader_load_choice");
        element.setTextContent("replace");
        ldrConfiguration.appendChild(element);*/
        element = document.createElement("loader_drop_and_create_table");
        element.setTextContent("yes");
        ldrConfiguration.appendChild(element);
    	
    	return dataFlow;
    }
    
}