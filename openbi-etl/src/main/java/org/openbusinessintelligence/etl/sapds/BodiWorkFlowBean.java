package org.openbusinessintelligence.etl.sapds;

import org.w3c.dom.*;

public class BodiWorkFlowBean {
	//private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(BodiWorkFlowBean.class.getPackage().getName());

	//  properties
	private String dataFlowName = "";
	private String workFlowName = "";
	
	// Internal objects
	private Element workFlow;
	private Element steps;
	private Element tryStep;
	private Element catchStep;
	private Element callStep;
	private Element script;
	private Element functioncallstep;
	private Element expression;
	
	
    // Constructor
    public BodiWorkFlowBean() {
        super();
    }

    // Set properties methods
	
	public void setDataFlowName(String property) {
		dataFlowName = property;
	}
	
	public void setWorkFlowName(String property) {
		workFlowName = property;
	}

    // Creation methods
    public Element getElement(Document document) throws Exception {
    	
    	// root element
    	workFlow = null;
    	workFlow = document.createElement("DIWorkflow");
    	workFlow.setAttribute("name", workFlowName);
    	document.appendChild(workFlow);

    	// add steps
    	steps = document.createElement("DISteps");
    	workFlow.appendChild(steps);
    	// add stat init script
    	/*script = document.createElement("DIScript");
    	steps.appendChild(script);
    	functioncallstep = document.createElement("DIFunctionCallStep");
    	script.appendChild(functioncallstep);
    	expression = document.createElement("DIExpression");
    	expression.setAttribute("isString", "true");
    	expression.setAttribute("expr", "FC_STAGE_STAT_INIT()");
    	functioncallstep.appendChild(expression);
    	// add try
    	tryStep = document.createElement("DITryStep");
    	steps.appendChild(tryStep);
    	steps = document.createElement("DISteps");
    	tryStep.appendChild(steps);*/
    	// add data flow
    	callStep = document.createElement("DICallStep");
    	callStep.setAttribute("calledObjectType", "Dataflow");
    	callStep.setAttribute("name", dataFlowName);
    	steps.appendChild(callStep);
    	// add stat final script
    	/*script = document.createElement("DIScript");
    	steps.appendChild(script);
    	functioncallstep = document.createElement("DIFunctionCallStep");
    	script.appendChild(functioncallstep);
    	expression = document.createElement("DIExpression");
    	expression.setAttribute("isString", "true");
    	expression.setAttribute("expr", "FC_STAGE_STAT_FINAL()");
    	functioncallstep.appendChild(expression);
    	// add catch
    	catchStep = document.createElement("DICatch");
    	catchStep.setAttribute("errorCode", "210101");
    	tryStep.appendChild(catchStep);
    	steps = document.createElement("DISteps");
    	catchStep.appendChild(steps);
    	// add stat final script
    	script = document.createElement("DIScript");
    	steps.appendChild(script);
    	functioncallstep = document.createElement("DIFunctionCallStep");
    	script.appendChild(functioncallstep);
    	expression = document.createElement("DIExpression");
    	expression.setAttribute("isString", "true");
    	expression.setAttribute("expr", "FC_STAGE_STAT_ERROR()");
    	functioncallstep.appendChild(expression);*/
		
    	return workFlow;
    }
}