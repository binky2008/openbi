package org.openbusinessintelligence.meta.execute;

import java.io.Writer;
import java.util.*;

import javax.persistence.*;

import org.openbusinessintelligence.core.file.*;
import org.openbusinessintelligence.meta.entity.*;

public class ScriptGenerator {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(ScriptGenerator.class.getPackage().getName());

	// Declarations of bean properties
	// Script properties
	private String scriptPrefix = "";
	private String scriptDirectory = "";
	
	// Metadata properties
	private String stageSourceCode = "";
    
    // Execution properties
    private int commitFrequency;

    // Constructor
    public ScriptGenerator() {
    	
    }
	
    // Set metadata properties methods
	public void setScriptPrefix(String property) {
		scriptPrefix = property;
	}
	
    // Set metadata properties methods
	public void setScriptDirectory(String property) {
		scriptDirectory = property;
	}
	
    // Set metadata properties methods
	public void setStageSourceCode(String property) {
		stageSourceCode = property;
	}

    // Set optional execution properties 
    public void setCommitFrequency(int cf) {
        commitFrequency = cf;
    }
	
	public void generateScripts() throws Exception {
		LOGGER.info("Generate stage1 scripts for source: " + stageSourceCode);
		
		FileImporter templateFile = new FileImporter();
		templateFile.setDirectoryName("scripts");
		templateFile.setFileName("template.sh");
		
		EntityManagerFactory emf = Persistence.createEntityManagerFactory("OpenBIStage");
		EntityManager em = emf.createEntityManager();
		Query query;
		
		query = em.createQuery("SELECT x FROM StageSource x WHERE x.etlStageSourceCode = ?1"); 
		query.setParameter(1, stageSourceCode);		
		List<StageSource> sources = query.getResultList();
		
		// get source objects and dbs
		List<StageObject> objects = sources.get(0).getStageObject();
		List<StageSourceDb> sourcedbs = sources.get(0).getStageSourceDb();
		
		// For each object
		String scriptName = "";
		String options = "";
		FileExporter scriptFile = new FileExporter();
		for (StageObject object:objects) {
			for (StageSourceDb sourcedb:sourcedbs) {
				if (scriptPrefix == null || scriptPrefix.equalsIgnoreCase("")) {
					scriptName = "";
				}
				else {
					scriptName = scriptPrefix + "_";
				}
				scriptName += stageSourceCode + "_" + object.getEtlStageObjectName();
				options = "-stagesourcecode " + stageSourceCode + " \\\\\n\t-stageobjectname " + object.getEtlStageObjectName();
				if (sourcedbs.size() > 1) {
					scriptName += "_" + sourcedb.getEtlStageDistributionCode();
					options += " \\\\\n\t-distributioncode " + sourcedb.getEtlStageDistributionCode();
				}
				if (commitFrequency > 0) {
					options += " \\\\\n\t-commitfrequency " + commitFrequency;
				}
				scriptName += ".sh";
				
				LOGGER.info("Generate script: " + scriptName);
				scriptFile.setDirectoryName(scriptDirectory);
				scriptFile.setFileName(scriptName);
				scriptFile.setContentString(templateFile.getString().replaceAll("#OPTIONS#", options));
				scriptFile.writeContentString();
			}
		}
		LOGGER.info("Generated stage1 scripts for source: " + stageSourceCode);
	}
}