package org.openbusinessintelligence.dblibrary;

import org.openbusinessintelligence.core.db.DBConnection;
import org.slf4j.LoggerFactory;

public class DBLibraryManager {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DBLibraryManager.class);
	
	private String className;
	private String name;
	
	Class<?> frameworkClass;
	DBLibrary framework;
	
	public void setFrameworkClass(String property) throws Exception {
		
		className = property;
		logger.info("Class name = " + className);

		frameworkClass = Class.forName(className);
		logger.info("Class created!");
		
		try {
			
			framework = (DBLibrary) frameworkClass.newInstance();
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
