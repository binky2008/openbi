package org.openbusinessintelligence.testframework;

import org.openbusinessintelligence.dbframework.*;

public class TestFrameworkImpl implements Framework {
	
	private String name = "test";
	public String[] modules;
	public String[] objects;

	public String getName() {
		
		return  name;
		
	}

	public String[] getModules() {
		
		return  modules;
		
	}

	public String[] getObjects() {
		
		return  objects;
		
	}

	public void install() {
		
	}

}
