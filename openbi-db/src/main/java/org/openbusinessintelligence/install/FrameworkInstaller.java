package org.openbusinessintelligence.install;

import java.io.*;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.openbusinessintelligence.core.db.ConnectionBean;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class FrameworkInstaller {    
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(FrameworkInstaller.class);

    private ConnectionBean connection = null;
    private String databaseType = "";
    private String module = "all";
	private String[] parameterNames;
	private String[] parameterValues;
	
    // Constructor
    public FrameworkInstaller() {
        super();
    }

    // Set connection
    public void setSourceConnection(ConnectionBean property) {
    	connection = property;
    }
    
    public void setDatabaseType(String property) {
    	databaseType = property;
    }
    
    public void setModule(String property) {
    	module = property;
    }
    
    public void setParameterNames(String[] property) {
    	parameterNames = property;
    }
    
    public void setParameterValues(String[] property) {
    	parameterValues = property;
    }

    public void install () throws Exception {
		
		org.w3c.dom.Document frameworkXML = null;
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			frameworkXML = docBuilder.parse(Thread.currentThread().getContextClassLoader().getResource("sql/" + databaseType + "/framework.xml").toString());
			frameworkXML.getDocumentElement().normalize();
		}
		catch(Exception e) {
			logger.error("Cannot load option file: " + e.getMessage());
			e.printStackTrace();
		    throw e;
		}
		
		NodeList modules;
		NodeList scripts;
		Node mNode;
		Node sNode;
		Element mElement;
		Element sElement;
		String script;
		String delimiter;
		
		Reader reader;
    	ScriptRunner runner;
    	ParameterReplacer replacer = new ParameterReplacer();

    	// Get modules
		modules = frameworkXML.getElementsByTagName("module");
		logger.debug("Found " + modules.getLength() + " modules");
		// Loop on modules
 		for (int m = 0; m < modules.getLength(); m++) {
 			mNode = modules.item(m);
 			if (mNode.getNodeType() == Node.ELEMENT_NODE) {
 				mElement = (Element) mNode;
 				if (
 					mElement.getAttribute("name").equalsIgnoreCase(module) ||
 					module.equalsIgnoreCase("all")
 				) {
 					logger.debug("Installing " + module + " module");
 					scripts = mElement.getElementsByTagName("script");
 					// Loop on scripts belonging to current module
 			 		for (int s = 0; s < scripts.getLength(); s++) {
 			 			sNode = scripts.item(s);
 			 			if (sNode.getNodeType() == Node.ELEMENT_NODE) {
 			 				sElement = (Element) sNode;
 			 				delimiter = sElement.getAttribute("delimiter");
 			 				script = sElement.getChildNodes().item(0).getNodeValue();
 		 					logger.debug("Script " + script + " delimiter " + delimiter);
 		 					
 		 					reader = Resources.getResourceAsReader("sql/" + databaseType + "/" + script);
 		 					// Substitute parameters
 		 					replacer.setSourceReader(reader);
 		 					replacer.setStringToReplace(parameterNames);
 		 					replacer.setStringReplacement(parameterValues);
 		 					reader = replacer.getTargetReader();
 		 					
 		 					// Run script
 		 					runner = new ScriptRunner(connection.getConnection());
 					    	if (delimiter.equalsIgnoreCase("/")) {
 					    		runner.setDelimiter("/");
 					    	}
 					    	else if (delimiter.equalsIgnoreCase(";")) {
 	 					    	runner.setDelimiter(";");
 					    	}
 					    	else {
 					    		runner.setSendFullScript(true);
 					    	}
 							runner.runScript(reader);
 							connection.getConnection().commit();
 							reader.close();
 		 					logger.debug("Script " + script + " executed");
 			 			}
 			 		}
 					logger.debug("Module " + module + " installed");
 				}
 			}
 		}
    }
}