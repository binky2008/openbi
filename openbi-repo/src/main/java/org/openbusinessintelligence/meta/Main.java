package org.openbusinessintelligence.meta;

import java.io.*;
import java.util.*;

import javax.xml.parsers.*;

import org.apache.commons.cli.*;
import org.openbusinessintelligence.meta.dictionary.*;
import org.openbusinessintelligence.meta.execute.*;
import org.openbusinessintelligence.meta.sapds.*;
import org.w3c.dom.*;

public class Main {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(Main.class.getPackage().getName());
	
	private static Options cmdOptions;
	private static Properties properties;
	private static CommandLine cmd;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOGGER.info("###################################################################");
		LOGGER.info("START");
		
		configureCmdOptions();
		CommandLineParser parser = new DefaultParser();
		try {
			// parse the command line arguments
			cmd = parser.parse(cmdOptions, args);
		}
		catch(Exception e) {
		    LOGGER.severe("Unexpected exception:" + e.getMessage());
		    throw e;
		}
		
	    if (cmd.hasOption("help")) {
	        // print help
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("obiTools",cmdOptions);
	    }
	    if (cmd.hasOption("propertyfile")) {
            try {
		    	properties = new Properties();
		    	properties.load(new FileInputStream(cmd.getOptionValue("propertyfile")));
            }
    		catch(Exception e) {
    			LOGGER.severe("Cannot read property file:\n" + e.getMessage());
    		    throw e;
    		}
	    }
	    if (cmd.hasOption("function")) {
	    	String function = cmd.getOptionValue("function");
	    	if (function.equalsIgnoreCase("checkcolums")) {
	    		ColumnChecker colCheckBean = new ColumnChecker();
	    		colCheckBean.setStageSourceCode(getOption("stagesourcecode"));
	    		
	    		colCheckBean.importColumns();
	    	}
	    	if (function.equalsIgnoreCase("importcolums")) {
	    		ColumnImporter colImportBean = new ColumnImporter();
	    		colImportBean.setStageSourceCode(getOption("stagesourcecode"));
	    		
	    		colImportBean.importColumns();
	    	}
	    	if (function.equalsIgnoreCase("generatescripts")) {
	    		ScriptGenerator generateScript = new ScriptGenerator();
	    		generateScript.setScriptPrefix(getOption("scriptprefix"));
	    		generateScript.setScriptDirectory(getOption("scriptdirectory"));
	    		generateScript.setStageSourceCode(getOption("stagesourcecode"));
	    		if (getOption("commitfrequency") != null) {
	    			generateScript.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));
	    		}
	    		
	    		generateScript.generateScripts();
	    	}
	    	if (function.equalsIgnoreCase("loadstage1")) {
	    		StageExecutor executeStage1Bean = new StageExecutor();
	    		executeStage1Bean.setStageSourceCode(getOption("stagesourcecode"));
	    		executeStage1Bean.setStageObjectName(getOption("stageobjectname"));
	    		executeStage1Bean.setDistributionCode(getOption("distributioncode"));
	    		executeStage1Bean.setTargetPropertyFile(getOption("stagetargetpropertyfile"));
	    		if (getOption("commitfrequency") != null) {
		    		executeStage1Bean.setCommitFrequency(Integer.parseInt(getOption("commitfrequency")));
	    		}
	    		executeStage1Bean.setPreserveDataOption(Boolean.parseBoolean(getOption("trgpreservedata")));
	    		
	    		executeStage1Bean.loadStage1();
	    	}
	    	if (function.equalsIgnoreCase("generatebodixml")) {
	    		SAPDSXMLCreator bodiXml = new SAPDSXMLCreator();
	    		bodiXml.setStageSourceCode(getOption("stagesourcecode"));
	    		bodiXml.setStageObjectName(getOption("stageobjectname"));
	    		bodiXml.setAbapDataFlowPrefix(getOption("bodiabapdataflowprefix"));
	    		bodiXml.setDataFlowPrefix(getOption("bodidataflowprefix"));
	    		bodiXml.setWorkFlowPrefix(getOption("bodiworkflowprefix"));
	    		bodiXml.setJobPrefix(getOption("bodijobprefix"));
	    		if (getOption("bodijobisabap").equalsIgnoreCase("Y")) {
		    		bodiXml.setIsAbap(true);
	    		}
	    		bodiXml.setExportFileName(getOption("bodiexportfile"));
	    		
	    		bodiXml.generate();
	    	}
	    }
	    
		LOGGER.info("FINISH");
		LOGGER.info("###################################################################");
	}
	
	private static void configureCmdOptions() throws Exception {
		
		cmdOptions = new Options();		
		Option help = new Option("help", "Print this message");
		cmdOptions.addOption(help);
		
		org.w3c.dom.Document optionsXML = null;
		try {
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			javax.xml.parsers.DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
			optionsXML = docBuilder.parse("cmd/stageCmdOptions.xml");
			optionsXML.getDocumentElement().normalize();
		}
		catch(Exception e) {
			LOGGER.severe("Cannot load option file:\n" + e.getMessage());
		    throw e;
		}
		NodeList nList = optionsXML.getElementsByTagName("option");
 		for (int temp = 0; temp < nList.getLength(); temp++) {
 			Node nNode = nList.item(temp);
 			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
 				Element eElement = (Element) nNode;
 				//System.out.println("Option : " + eElement.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue());
 				Option option = OptionBuilder.hasArg()
 						.withArgName(eElement.getElementsByTagName("argName").item(0).getChildNodes().item(0).getNodeValue())
 		                .withDescription(eElement.getElementsByTagName("description").item(0).getChildNodes().item(0).getNodeValue())
 		                .create(eElement.getElementsByTagName("name").item(0).getChildNodes().item(0).getNodeValue());
 				cmdOptions.addOption(option);
		   }
		}
	}
	
	private static String getOption(String optionName) {
		String optionValue = null;

		if (
			cmd.getOptionValue(optionName) == null ||
			cmd.getOptionValue(optionName).equalsIgnoreCase("")
		) {
			try {
				optionValue = properties.getProperty(optionName);			
			}
			catch(NullPointerException npe) {
				
			}
		}
		else {
			optionValue = cmd.getOptionValue(optionName);
		}
		
		return optionValue;
	}
}
