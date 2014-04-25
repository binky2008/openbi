package org.openbusinessintelligence.load.meta;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.util.*;

import javax.persistence.*;

import org.openbusinessintelligence.core.db.*;
import org.openbusinessintelligence.load.entity.*;

public class ColumnImportBean {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(ColumnImportBean.class.getPackage().getName());

    // Declarations of bean properties
	// Metadata properties
	private String stageSourceCode = "";
	
	// Source properties
    private String sourceName = "";

    // Constructor
    public ColumnImportBean() {
    	
    }
    
    // Set metadata properties methods
	public void setStageSourceCode(String property) {
		stageSourceCode = property;
	}
    
    public void setSourceName(String property) {
        sourceName = property;
    }
    
	public void importColumns() throws Exception {
		LOGGER.info("List of objects for source: " + stageSourceCode);
		EntityManagerFactory emf = Persistence.createEntityManagerFactory("OpenBIStage");
		EntityManager em = emf.createEntityManager();
		Query query;
		
		// get source
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
		org.openbusinessintelligence.core.db.ConnectionBean sourceConnectionBean = new org.openbusinessintelligence.core.db.ConnectionBean();
		sourceConnectionBean.setPropertyFile(dbproperties.getProperty("srcconnaddpropertyfile"));
		sourceConnectionBean.setDatabaseDriver(dbproperties.getProperty("srcdbdriverclass"));
		sourceConnectionBean.setConnectionURL(dbproperties.getProperty("srcdbconnectionurl"));
		sourceConnectionBean.setUserName(dbproperties.getProperty("srcdbusername"));
		sourceConnectionBean.setPassWord(dbproperties.getProperty("srcdbpassword"));
		sourceConnectionBean.openConnection();
		
		TableDictionaryBean dataDict = new TableDictionaryBean();
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
			String[] colOriginalDefs = dataDict.getColumnDefinition();
			int[] colPkPos = dataDict.getColumnPkPositions();

			em.getTransaction().begin();

			for (int i=0; i<colNames.length; i++) {
				query = em.createQuery("SELECT x FROM StageColumn x WHERE x.etlStageObjectId = ?1 AND x.etlStageColumnName = ?2"); 
				query.setParameter(1, object.getEtlStageObjectId());
				query.setParameter(2, colNames[i]);
				List<StageColumn> columns = query.getResultList();
				
				StageColumn column;
				if (columns.size() == 0) {
					column = new StageColumn();
					column.setEtlStageObjectId(BigDecimal.valueOf(object.getEtlStageObjectId()));
					column.setEtlStageColumnPos(BigDecimal.valueOf(i+1));
					column.setEtlStageColumnName(colNames[i]);
					column.setEtlStageColumnDef(colDefs[i]);
					column.setEtlStageColumnDefSrc(colOriginalDefs[i]);
					column.setEtlStageColumnEdwhFlag(BigDecimal.valueOf(1));
					if (colPkPos[i]>0) {
						column.setEtlStageColumnNkPos(BigDecimal.valueOf(colPkPos[i]));
						object.setEtlStageSourceNkFlag(BigDecimal.valueOf(1));
					}
					em.persist(column);
				}
				else {
					column = columns.get(0);
					column.setEtlStageColumnPos(BigDecimal.valueOf(i+1));
					column.setEtlStageColumnDef(colDefs[i]);
					column.setEtlStageColumnDefSrc(colOriginalDefs[i]);
					if (colPkPos[i]>0) {
						column.setEtlStageColumnNkPos(BigDecimal.valueOf(colPkPos[i]));
						object.setEtlStageSourceNkFlag(BigDecimal.valueOf(1));
					}
				}
			}
			em.getTransaction().commit();
		}
	}
}
