package org.openbusinessintelligence.install;

import java.io.*;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.openbusinessintelligence.core.db.ConnectionBean;
import org.openbusinessintelligence.core.file.FileInputBean;
import org.openbusinessintelligence.core.script.InstallScriptCreator;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class InstallFramework {    
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(InstallFramework.class);

    private ConnectionBean connection = null;
    private String databaseType = "";
    private String module = "tool";
	
    // Constructor
    public InstallFramework() {
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

		modules = frameworkXML.getElementsByTagName("module");
 		for (int m = 0; m < modules.getLength(); m++) {
 			mNode = modules.item(m);
 			if (mNode.getNodeType() == Node.ELEMENT_NODE) {
 				mElement = (Element) mNode;
 				if (mElement.getAttribute("name").equalsIgnoreCase(module)) {
 					logger.debug("Installing " + module + " module");
 					scripts = mElement.getElementsByTagName("script");
 			 		for (int s = 0; s < scripts.getLength(); s++) {
 			 			sNode = scripts.item(s);
 			 			if (sNode.getNodeType() == Node.ELEMENT_NODE) {
 			 				sElement = (Element) sNode;
 			 				delimiter = sElement.getAttribute("delimiter");
 			 				script = sElement.getChildNodes().item(0).getNodeValue();
 		 					logger.debug("Script " + script + " delimiter " + delimiter);
 		 					
 		 					reader = Resources.getResourceAsReader("sql/" + databaseType + "/" + script);
 					    	runner = new ScriptRunner(connection.getConnection());
 					    	runner.setSendFullScript(true);
 							runner.runScript(reader);
 							connection.getConnection().commit();
 							reader.close();
 			 			}
 			 		}
 				}
 			}
 		}
    	
    	/*Reader reader;
    	ScriptRunner runner;
    	
    	BufferedReader installScript = new BufferedReader(Resources.getResourceAsReader("sql/" + databaseType + "/install_mesr.sql"));
		String installLine;
		
		if (databaseType.equalsIgnoreCase("oracle")) {
			while ((installLine=installScript.readLine()) != null) {
				if (installLine.length()>0) {
					if (installLine.trim().substring(0,1).equals("@")) {
						installLine = installLine.trim().substring(1);
						if (installLine.substring(installLine.length()-1).equals(";")) {
							installLine = installLine.substring(0,installLine.length()-1);
						}
						logger.debug("Script " + installLine + " added");
				    	reader = Resources.getResourceAsReader("sql/" + databaseType + "/" + installLine);
				    	
				    	runner = new ScriptRunner(connection.getConnection());

						runner.runScript(reader);
						connection.getConnection().commit();
						reader.close();
					}
				}
			}
		}*/
    }
}