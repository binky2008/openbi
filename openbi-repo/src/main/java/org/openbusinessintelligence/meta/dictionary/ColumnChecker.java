package org.openbusinessintelligence.meta.dictionary;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.util.*;

import javax.persistence.*;

import org.openbusinessintelligence.core.db.*;
import org.openbusinessintelligence.meta.entity.*;

public class ColumnChecker {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(ColumnChecker.class.getPackage().getName());

    // Declarations of bean properties
	// Metadata properties
	private String stageSourceCode = "";
	
	// Source properties
    private String sourceName = "";

    // Constructor
    public ColumnChecker() {
    	
    }
	
    // Set metadata properties methods
	public void setStageSourceCode(String property) {
		stageSourceCode = property;
	}
	
    public void setSourceName(String property) {
        sourceName = property;
    }
    
	public void importColumns() throws Exception {
		LOGGER.info("Import columns for source: " + stageSourceCode);
		EntityManagerFactory emf = Persistence.createEntityManagerFactory("OpenBIStage");
		EntityManager em = emf.createEntityManager();
		Query query;
		
		query = em.createQuery("SELECT x FROM StageSource x WHERE x.etlStageSourceCode = ?1"); 
		query.setParameter(1, stageSourceCode);		
		List<StageSource> sources = query.getResultList();
		
		// get source objects and dbs
		List<StageObject> objects = sources.get(0).getStageObject();
		List<StageSourceDb> sourcedbs = sources.get(0).getStageSourceDb();

		// load properties from property file
		String dbpropertyfile = "datasources/" + sourcedbs.get(0).getEtlStageSourceDbJdbcname() + ".properties";
		Properties dbproperties = new Properties();
		dbproperties.load(new FileInputStream(dbpropertyfile));
		
		// Configure db connection
		org.openbusinessintelligence.core.db.DBConnection sourceConnectionBean = new org.openbusinessintelligence.core.db.DBConnection();
		sourceConnectionBean.setPropertyFile(dbproperties.getProperty("srcconnaddpropertyfile"));
		sourceConnectionBean.setDatabaseDriver(dbproperties.getProperty("srcdbdriverclass"));
		sourceConnectionBean.setConnectionURL(dbproperties.getProperty("srcdbconnectionurl"));
		sourceConnectionBean.setUserName(dbproperties.getProperty("srcdbusername"));
		sourceConnectionBean.setPassWord(dbproperties.getProperty("srcdbpassword"));
		sourceConnectionBean.openConnection();

		DictionaryExtractor dataDict = new DictionaryExtractor();
		dataDict.setSourceConnection(sourceConnectionBean);

		// For each object
		String sourceIdentifier;
		for (StageObject object:objects) {

			// Dermine source identifier
			sourceIdentifier = object.getEtlStageObjectName();
			if (!(sourcedbs.get(0).getEtlStageSourceOwner().equals("")) && sourcedbs.get(0).getEtlStageSourceOwner() != null) {
				sourceIdentifier = sourcedbs.get(0).getEtlStageSourceOwner() + "." + sourceIdentifier;
			}
			
			dataDict.setSourceTable(sourceIdentifier);
			dataDict.retrieveColumns();
			
			String[] colNames = dataDict.getColumnNames();
			String[] colDefs = dataDict.getColumnDefinition();
			int[] colPkPos = dataDict.getColumnPkPositions();

			em.getTransaction().begin();
			query = em.createQuery("DELETE FROM StageColumnCheck x WHERE x.etlStageObjectId = ?1"); 
			query.setParameter(1, object.getEtlStageObjectId());
			query.executeUpdate();

			for (int i=0; i<colNames.length; i++) {
				StageColumnCheck newCol = new StageColumnCheck();
				newCol.setEtlStageObjectId(BigDecimal.valueOf(object.getEtlStageObjectId()));
				newCol.setEtlStageColumnPos(BigDecimal.valueOf(i+1));
				newCol.setEtlStageColumnName(colNames[i]);
				newCol.setEtlStageColumnDef(colDefs[i]);
				if (colPkPos[i]>0) {
					newCol.setEtlStageColumnNkPos(BigDecimal.valueOf(colPkPos[i]));
				}
				em.persist(newCol);
			}
			em.getTransaction().commit();
		}
		LOGGER.info("Imported columns for source: " + stageSourceCode);
	}
}
