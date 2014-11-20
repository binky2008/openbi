package org.openbusinessintelligence.dbframework;

import org.openbusinessintelligence.core.db.ConnectionBean;
import org.slf4j.LoggerFactory;

public class FrameworkManager {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(FrameworkManager.class);
	
	private String className;
	private String name;
	
	Class<?> frameworkClass;
	Framework framework;
	
	public void setFrameworkClass(String property) throws Exception {
		
		className = property;
		logger.info("Class name = " + className);

		frameworkClass = Class.forName(className);
		logger.info("Class created!");
		
		try {
			
			framework = (Framework) frameworkClass.newInstance();
			logger.info("Class instanciated!");
			
		}
		catch (Exception e) {
			
			logger.error("UNEXPECTED EXCEPTION: " + e.getMessage());
			e.printStackTrace();
			
			throw e;
		}
	}
	
	public String getName() {
		
		logger.debug("Name = " + framework.getName());
		return framework.getName();
	}
	
}
