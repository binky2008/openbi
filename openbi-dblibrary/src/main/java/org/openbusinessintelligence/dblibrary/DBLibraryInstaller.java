package org.openbusinessintelligence.dblibrary;

import java.io.*;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.openbusinessintelligence.core.db.DBConnection;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class DBLibraryInstaller {    
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DBLibraryInstaller.class);

    private DBConnection connection = null;
    private String databaseProduct = "";
    private String catalog = "";
    private String schema = "";
    private String module = "all";
	private String[] parameterNames;
	private String[] parameterValues;
	
    // Constructor
    public DBLibraryInstaller() {
        super();
    }

    // Set connection
    public void setSourceConnection(DBConnection property) {
    	connection = property;
    }
    
    public void setDatabaseProduct(String property) {
    	databaseProduct = property;
    }
    
    public void setCatalog(String property) {
    	catalog = property;
    }
    
    public void setSchema(String property) {
    	schema = property;
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
		
    	String path = "sql/" + databaseProduct;
    	if (catalog != null && !catalog.equals("")) {
    		path += "/" + catalog;
    	}
    	if (schema != null && !schema.equals("")) {
    		path += "/" + schema;
    	}
    	
		org.w3c.dom.Document frameworkXML = null;
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			frameworkXML = docBuilder.parse(Thread.currentThread().getContextClassLoader().getResource(path + "/framework.xml").toString());
			frameworkXML.getDocumentElement().normalize();
		}
		catch(Exception e) {
			logger.error("Cannot load option file: " + path + "/framework.xml");
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
    	DBLibraryReplacer replacer = new DBLibraryReplacer();

    	// Get modules
		modules = frameworkXML.getElementsByTagName("module");
		logger.debug("Found " + modules.getLength() + " modules");
		// Loop on modules
 		for (int m = 0; m < modules.getLength(); m++) {
 			mNode = modules.item(m);
 			if (mNode.getNodeType() == Node.ELEMENT_NODE) {
 				mElement = (Element) mNode;
				logger.debug("Found module \"" + mElement.getAttribute("name") + "\"");
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
 		 					
 		 					reader = Resources.getResourceAsReader(path + "/" + script);
 		 					// Substitute parameters
 		 					replacer.setSourceReader(reader);
 		 					replacer.setStringToReplace(parameterNames);
 		 					replacer.setStringReplacement(parameterValues);
 		 					reader = replacer.getTargetReader();
 		 					
 		 					// Run script
 		 					runner = new ScriptRunner(connection.getConnection());
 					    	if (delimiter != null && !delimiter.equals("")) {
 					    		runner.setDelimiter(delimiter);
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
 				else {
 					logger.debug("Module " + module + " not to be installed");
 				}
 			}
 		}
    }
}