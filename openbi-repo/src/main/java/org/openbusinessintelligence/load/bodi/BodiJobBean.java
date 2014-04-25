package org.openbusinessintelligence.load.bodi;

import org.w3c.dom.*;

public class BodiJobBean {
	//private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(BodiJobBean.class.getPackage().getName());

	//  properties
	//private String dataFlowName = "";
	private String workFlowName = "";
	private String jobName = "";
	private String stageSourceCode = "";
	private String stageObjectName = "";
	
	// Internal objects
	private Element job;
	private Element variables;
	private Element element;
	private Element steps;
	/*private Element tryStep;
	private Element catchStep;*/
	private Element callStep;
	/*private Element script;
	private Element functioncallstep;
	private Element expression;*/
	private Element attributes;
	private Element attribute;
	
	
    // Constructor
    public BodiJobBean() {
        super();
    }

    // Set properties methods
	
	/*public void setDataFlowName(String property) {
		dataFlowName = property;
	}*/
	
	public void setWorkFlowName(String property) {
		workFlowName = property;
	}
	
	public void setJobName(String property) {
		jobName = property;
	}
	
	public void setStageSourceCode(String property) {
		stageSourceCode = property;
	}
	
	public void setStageObjectName(String property) {
		stageObjectName = property;
	}

    // Creation methods
    public Element getElement(Document document) throws Exception {
    	
    	// root element
    	job = null;
    	job = document.createElement("DIJob");
    	job.setAttribute("name", jobName);
    	document.appendChild(job);
    	
    	// add variables
    	variables = document.createElement("DIVariables");
    	job.appendChild(variables);
    	// $GV_STAT_ID
    	element = document.createElement("DIElement");
    	element.setAttribute("paramType", "GLOBAL");
    	element.setAttribute("datatype", "DECIMAL");
    	element.setAttribute("precision", "10");
    	element.setAttribute("scale", "0");
    	element.setAttribute("name", "$GV_STAT_ID");
    	variables.appendChild(element);
    	// $GV_GUI
    	element = document.createElement("DIElement");
    	element.setAttribute("paramType", "GLOBAL");
    	element.setAttribute("datatype", "DECIMAL");
    	element.setAttribute("precision", "10");
    	element.setAttribute("scale", "0");
    	element.setAttribute("name", "$GV_GUI");
    	variables.appendChild(element);
    	// $GV_RESULT
    	element = document.createElement("DIElement");
    	element.setAttribute("paramType", "GLOBAL");
    	element.setAttribute("datatype", "DECIMAL");
    	element.setAttribute("precision", "10");
    	element.setAttribute("scale", "0");
    	element.setAttribute("name", "$GV_RESULT");
    	variables.appendChild(element);
    	// $GV_STEP_NO
    	element = document.createElement("DIElement");
    	element.setAttribute("paramType", "GLOBAL");
    	element.setAttribute("datatype", "DECIMAL");
    	element.setAttribute("precision", "10");
    	element.setAttribute("scale", "0");
    	element.setAttribute("name", "$GV_STEP_NO");
    	variables.appendChild(element);
    	// $GV_STAGE_SOURCE
    	element = document.createElement("DIElement");
    	element.setAttribute("paramType", "GLOBAL");
    	element.setAttribute("datatype", "VARCHAR");
    	element.setAttribute("size", "10");
    	element.setAttribute("name", "$GV_STAGE_SOURCE");
    	variables.appendChild(element);
    	// $GV_STAGE_OBJECT
    	element = document.createElement("DIElement");
    	element.setAttribute("paramType", "GLOBAL");
    	element.setAttribute("datatype", "VARCHAR");
    	element.setAttribute("size", "100");
    	element.setAttribute("name", "$GV_STAGE_OBJECT");
    	variables.appendChild(element);
    	// $GV_INCREMENT_BOUND
    	element = document.createElement("DIElement");
    	element.setAttribute("paramType", "GLOBAL");
    	element.setAttribute("datatype", "DECIMAL");
    	element.setAttribute("precision", "22");
    	element.setAttribute("scale", "0");
    	element.setAttribute("name", "$GV_INCREMENT_BOUND");
    	variables.appendChild(element);

    	// add steps
    	steps = document.createElement("DISteps");
    	job.appendChild(steps);

    	// add workflow
    	callStep = document.createElement("DICallStep");
    	callStep.setAttribute("calledObjectType", "Workflow");
    	callStep.setAttribute("name", workFlowName);
    	steps.appendChild(callStep);
    	
    	// Set variables
    	attributes = document.createElement("DIAttributes");
    	job.appendChild(attributes);
    	attribute = document.createElement("DIAttribute");
    	attribute.setAttribute("name", "job_GV_$GV_STAGE_SOURCE");
    	attribute.setAttribute("value", "'" + stageSourceCode + "'");
    	attributes.appendChild(attribute);
    	attribute = document.createElement("DIAttribute");
    	attribute.setAttribute("name", "job_GV_$GV_STAGE_OBJECT");
    	attribute.setAttribute("value", "'" + stageObjectName + "'");
    	attributes.appendChild(attribute);
    	attribute = document.createElement("DIAttribute");
    	attribute.setAttribute("name", "job_monitor_sample_rate");
    	attribute.setAttribute("value", "0");
    	attributes.appendChild(attribute);
    	
    	return job;
    }
    
}