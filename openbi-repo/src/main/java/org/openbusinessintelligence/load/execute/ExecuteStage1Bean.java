package org.openbusinessintelligence.load.execute;

import java.io.*;
import java.util.*;

import javax.persistence.*;

import org.openbusinessintelligence.core.db.*;
import org.openbusinessintelligence.load.entity.*;
import org.openbusinessintelligence.load.meta.*;

public class ExecuteStage1Bean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(ExecuteStage1Bean.class.getPackage().getName());

	// Declarations of bean properties
	// Metadata properties
	private String stageSourceCode = "";
	private String stageObjectName = "";
	private String distributionCode = "";
	private String targetPropertyFile = "";
    
    // Execution properties
    private int commitFrequency;
    private boolean preserveDataOption = false;
	
	// Source properties
    private String sourceName = "";

    // Constructor
    public ExecuteStage1Bean() {
    	
    }
	
    // Set metadata properties methods
	public void setStageSourceCode(String property) {
		stageSourceCode = property;
	}
	
	public void setStageObjectName(String property) {
		stageObjectName = property;
	}
	
	public void setDistributionCode(String property) {
		distributionCode = property;
	}
	
    public void setSourceName(String property) {
        sourceName = property;
    }
    
    public void setTargetPropertyFile(String property) {
    	targetPropertyFile = property;
    }


    // Set optional execution properties 
    public void setCommitFrequency(int cf) {
        commitFrequency = cf;
    }
    
	public void setPreserveDataOption(boolean tt) {
		preserveDataOption = tt;
	}
    
    
    // Execution methods
    public void loadStage1() throws Exception {
		LOGGER.info("List Stage1 tables for source: " + stageSourceCode);
		EntityManagerFactory emf = Persistence.createEntityManagerFactory("OpenBIStage");
		EntityManager em = emf.createEntityManager();
		Query query;
		
		query = em.createQuery("SELECT x FROM StageSource x WHERE x.etlStageSourceCode = ?1"); 
		query.setParameter(1, stageSourceCode);
		List<StageSource> sources = query.getResultList();
		
		// get source objects and dbs
		List<StageObject> objects = sources.get(0).getStageObject();
		List<StageSourceDb> sourcedbs = sources.get(0).getStageSourceDb();

		// Target properties
		String trgdbpropertyfile;
		Properties trgdbproperties;
		trgdbpropertyfile = "datasources/" + targetPropertyFile + ".properties";
		trgdbproperties = new Properties();
		trgdbproperties.load(new FileInputStream(trgdbpropertyfile));

		// For each object
		String srcdbpropertyfile;
		Properties srcdbproperties;
		String sourceIdentifier;
		String[] srcMapColumns;
		String[] trgMapColumns;
		String[] defaultColumns = null;
		String[] defaultValues = null;
		DataCopyBean dataCopy = new DataCopyBean();
		dataCopy.setCommitFrequency(commitFrequency);
		if (sourcedbs.size() > 1) {
			defaultColumns = new String[1];
			defaultValues = new String[1];
			dataCopy.setPreserveDataOption(true);
		}
		for (StageObject object:objects) {
			if (stageObjectName == null || stageObjectName.equals("") || object.getEtlStageObjectName().equalsIgnoreCase(stageObjectName)) {
				LOGGER.info("Source: " + stageSourceCode + " - Object: " + object.getEtlStageObjectName() + " - BEGIN");
				query = em.createQuery("SELECT x FROM StageColumn x WHERE x.etlStageObjectId = ?1 AND x.etlStageColumnNameMap <> x.etlStageColumnName AND LENGTH(x.etlStageColumnNameMap)>0");
				query.setParameter(1, object.getEtlStageObjectId()); 
				List<StageColumn> columns = query.getResultList();
				srcMapColumns = new String[columns.size()];
				trgMapColumns = new String[columns.size()];
				int i = 0;
				for (StageColumn column:columns) {
					srcMapColumns[i] = column.getEtlStageColumnName();
					trgMapColumns[i] = column.getEtlStageColumnNameMap();
					i++;
				}
				for (StageSourceDb sourcedb:sourcedbs) {
					if (distributionCode == null  || distributionCode.equals("") || sourcedb.getEtlStageDistributionCode().equalsIgnoreCase(distributionCode)) {
						
						if (sourcedbs.size() > 1) {
							LOGGER.info("Object: " + object.getEtlStageObjectName() + " - " + sourcedb.getEtlStageDistributionCode());
							defaultValues[0] = sourcedb.getEtlStageDistributionCode();
							defaultColumns[0] = "DI_REGION_ID";
						}
						
						// Dermine source identifier
						sourceIdentifier = object.getEtlStageObjectName();
						if (!(sourcedb.getEtlStageSourceOwner().equalsIgnoreCase("")) && sourcedb.getEtlStageSourceOwner() != null) {
							sourceIdentifier = sourcedb.getEtlStageSourceOwner() + "." + sourceIdentifier;
						}
						// load properties from property file
						srcdbpropertyfile = "datasources/" + sourcedb.getEtlStageSourceDbJdbcname() + ".properties";
						srcdbproperties = new Properties();
						srcdbproperties.load(new FileInputStream(srcdbpropertyfile));
						
			    		org.openbusinessintelligence.core.db.ConnectionBean sourceConnectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
			    		sourceConnectionBean.setPropertyFile(srcdbproperties.getProperty("srcconnaddpropertyfile"));
			    		sourceConnectionBean.setDatabaseDriver(srcdbproperties.getProperty("srcdbdriverclass"));
			    		sourceConnectionBean.setConnectionURL(srcdbproperties.getProperty("srcdbconnectionurl"));
			    		sourceConnectionBean.setUserName(srcdbproperties.getProperty("srcdbusername"));
			    		sourceConnectionBean.setPassWord(srcdbproperties.getProperty("srcdbpassword"));
			    		sourceConnectionBean.openConnection();

			    		org.openbusinessintelligence.core.db.ConnectionBean targetConnectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
			    		targetConnectionBean.setPropertyFile(srcdbproperties.getProperty("srcconnaddpropertyfile"));
			    		targetConnectionBean.setDatabaseDriver(srcdbproperties.getProperty("srcdbdriverclass"));
			    		targetConnectionBean.setConnectionURL(srcdbproperties.getProperty("srcdbconnectionurl"));
			    		targetConnectionBean.setUserName(srcdbproperties.getProperty("srcdbusername"));
			    		targetConnectionBean.setPassWord(srcdbproperties.getProperty("srcdbpassword"));
			    		targetConnectionBean.openConnection();

			    		dataCopy.setSourceConnection(sourceConnectionBean);
						dataCopy.setSourceTable(sourceIdentifier);
			    		dataCopy.setTargetConnection(sourceConnectionBean);
						dataCopy.setTargetTable(object.getEtlStageStg1TableName());

						dataCopy.setSourceMapColumns(srcMapColumns);
						dataCopy.setTargetMapColumns(trgMapColumns);
						
						dataCopy.setTargetDefaultColumns(defaultColumns);
						dataCopy.setTargetDefaultValues(defaultValues);
						
						dataCopy.retrieveColumnList();
						dataCopy.executeSelect();
						dataCopy.executeInsert();
					}
				}
				LOGGER.info("Source: " + stageSourceCode + " - Object: " + object.getEtlStageObjectName() + " - END");
			}
		}
		LOGGER.info("Stage1 tables loaded for source: " + stageSourceCode);
    }
}
