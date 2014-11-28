package org.openbusinessintelligence.testframework;

import org.openbusinessintelligence.dblibrary.*;

public class TestDBLibraryImpl implements DBLibrary {
	
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
