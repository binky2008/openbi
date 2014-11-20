package org.openbusinessintelligence.dbframework;

public interface Framework {

	public String getName();
	
	public String[] getModules();

	public String[] getObjects();

	public void install();

}
