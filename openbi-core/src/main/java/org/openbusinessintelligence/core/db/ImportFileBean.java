package org.openbusinessintelligence.core.db;

import org.slf4j.LoggerFactory;

public class ImportFileBean {
	
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(ImportFileBean.class);

	private String databaseDriver = "";
	private String connectionURL = "";
	private String userName = "";
	private String passWord = "";
	private String sourceName = "";
	
	private String targetTable = "";
	private String fileNameColumn = "";
	private String fileContentColumn = "";
	
	/**
	 * Constructor
	 */
	public ImportFileBean() {
		super();
	}

	/**
	 * Set the database JDBC driver
	 */
	public void setDatabaseDriver(String dd) {
		databaseDriver = dd;
	}

	/**
	 * Set the database connection URL (used if no data sorce given)
	 */
	public void setConnectionURL(String cu) {
		connectionURL = cu;
	}

	/**
	 * Set the connection username (used if no data sorce given)
	 */
	public void setUserName(String un) {
		userName = un;
	}

	/**
	 * Set the connection password (used if no data sorce given)
	 */
	public void setPassWord(String pw) {
		passWord = pw;
	}

	/**
	 * Set the data source name
	 */
	public void setSourceName(String sn) {
		sourceName = sn;
	}

	/**
	 * Set the target table
	 */
	public void setTargetTable(String tt) {
		sourceName = tt;
	}

	/**
	 * Set the target column for the file name
	 */
	public void setFileNameColumn(String fnc) {
		fileNameColumn = fnc;
	}

	/**
	 * Set the target column for the file content
	 */
	public void setFileContentColumn(String fcc) {
		fileContentColumn = fcc;
	}
}
