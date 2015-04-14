package org.openbusinessintelligence.meta.sapds;

import java.io.*;
import java.util.*;

import javax.persistence.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.*;

import org.w3c.dom.*;
import org.openbusinessintelligence.meta.entity.*;

public class SAPDSXMLCreator {

	private final static java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(SAPDSXMLCreator.class.getPackage().getName());

	// Declarations of bean properties
	// Metadata properties
	private String stageSourceCode = "";
	private String stageObjectName = "";
	//private String distributionCode = "";
	
	// Bodi properties
	private String abapDataFlowPrefix = "";
	private String dataFlowPrefix = "";
	private String workFlowPrefix = "";
	private String jobPrefix = "";
	private boolean isAbap = false;
	
	// File properties
	private String exportFileName = "";

    // Constructor
    public SAPDSXMLCreator() {
    	
    }
	
    // Set metadata properties methods
	public void setStageSourceCode(String property) {
		stageSourceCode = property;
	}
	
	public void setStageObjectName(String property) {
		stageObjectName = property;
	}
	
	/*public void setDistributionCode(String property) {
		distributionCode = property;
	}*/
	
	public void setAbapDataFlowPrefix(String property) {
		abapDataFlowPrefix = property;
	}
	
	public void setDataFlowPrefix(String property) {
		dataFlowPrefix = property;
	}
	
	public void setWorkFlowPrefix(String property) {
		workFlowPrefix = property;
	}
	
	public void setJobPrefix(String property) {
		jobPrefix = property;
	}
	
	public void setIsAbap(boolean property) {
		isAbap = property;
	}
	
    // File properties methods
	public void setExportFileName(String property) {
		exportFileName = property;
	}
    
    
    // Execution methods
    public void generate() throws Exception {
		LOGGER.info("Generate stage1 bodi objects for source: " + stageSourceCode);
		EntityManagerFactory emf = Persistence.createEntityManagerFactory("OpenBIStage");
		EntityManager em = emf.createEntityManager();
		Query query;
		
		query = em.createQuery("SELECT x FROM StageSource x WHERE x.etlStageSourceCode = ?1"); 
		query.setParameter(1, stageSourceCode);
		List<StageSource> sources = query.getResultList();
		
		// get source objects and dbs
		List<StageObject> objects = sources.get(0).getStageObject();
		List<StageSourceDb> sourcedbs = sources.get(0).getStageSourceDb();

		// Help variables
		String identifierRoot;
		String sourceOwner;
		
		// Column lists
		String[] srcColumns;
		String[] trgColumns;
		String[] trgTypes;
		String[] defaultColumns = null;
		String[] defaultValues = null;
		String[] defaultColTypes = null;
		if (sourcedbs.size() > 1) {
			defaultColumns = new String[1];
			defaultValues = new String[1];
			defaultColTypes = new String[1];
		}
		else {
			defaultColumns = new String[0];
			defaultValues = new String[0];
			defaultColTypes = new String[0];
		}

		// Create xml document objects
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document document = documentBuilder.newDocument();
		Element export = document.createElement("DataIntegratorExport");
		//Element exportParam = document.createElement("DataIntegratorExport");
		// For each object
		for (StageObject object:objects) {
			if (stageObjectName == null || stageObjectName.equals("") || object.getEtlStageObjectName().equalsIgnoreCase(stageObjectName)) {
				LOGGER.info("Source: " + stageSourceCode + " - Object: " + object.getEtlStageObjectName() + " - BEGIN");
				query = em.createQuery("SELECT x FROM StageColumn x WHERE x.etlStageObjectId = ?1 ORDER BY x.etlStageColumnPos");
				query.setParameter(1, object.getEtlStageObjectId()); 
				List<StageColumn> columns = query.getResultList();
				srcColumns = new String[columns.size()];
				trgColumns = new String[columns.size()];
				trgTypes = new String[columns.size()];
				String incrColumn;
				String incrColType;
				int i = 0;
				for (StageColumn column:columns) {
					srcColumns[i] = column.getEtlStageColumnName();
					if (column.getEtlStageColumnNameMap() == null || column.getEtlStageColumnNameMap().equals("")) {
						trgColumns[i] = column.getEtlStageColumnName();
					}
					else {
						trgColumns[i] = column.getEtlStageColumnNameMap();
					}
					trgTypes[i] = column.getEtlStageColumnDef();
					i++;
					
					/*if (column.getEtlStageColumnIncrFlag().intValue() > 0) {
						
					}*/
					
				}
				if (sourcedbs.size() > 1) {
					defaultColumns[0] = "DI_REGION_ID";
					defaultColTypes[0] = "VARCHAR(12)";
					defaultValues[0] = "current_system_configuration()";
				}
				
				if (!(sourcedbs.get(0).getEtlStageSourceOwner() == null) && sourcedbs.get(0).getEtlStageSourceOwner().split(":").length>1) {
					sourceOwner = sourcedbs.get(0).getEtlStageSourceOwner().split(":")[1];
				}
				else {
					sourceOwner = sourcedbs.get(0).getEtlStageSourceOwner();
				}
				
				if (isAbap) {
					// Generate and append abap dataflow
					SAPDSAbapDataFlowBean abapDataFlow = new SAPDSAbapDataFlowBean();
					abapDataFlow.setSourceDataStore(sources.get(0).getEtlStageSourceBodiDs());
					abapDataFlow.setDataFlowName(abapDataFlowPrefix + "_" + object.getEtlStageStg2ViewName());
					abapDataFlow.setSourceTableName(object.getEtlStageObjectName());
					export.appendChild(abapDataFlow.getElement(document));
					// Generate and append dataflow
					SAPDSDataFlowForAbapBean dataFlow = new SAPDSDataFlowForAbapBean();
					dataFlow.setAbapDataFlowName(abapDataFlowPrefix + "_" + object.getEtlStageStg2ViewName());
					dataFlow.setTargetDataStore(sources.get(0).getEtlStageBodiDs());
					dataFlow.setTargetOwnerName(sources.get(0).getEtlStageOwner());
					dataFlow.setTargetTableName(object.getEtlStageStg1TableName());
					dataFlow.setDataFlowName(dataFlowPrefix + "_" + object.getEtlStageStg2ViewName());
					export.appendChild(dataFlow.getElement(document));
				}
				else {
					// Generate and append source table
					SAPDSTable sourceTable = new SAPDSTable();
					sourceTable.setDataStore(sources.get(0).getEtlStageSourceBodiDs());
					sourceTable.setOwnerName(sourceOwner);
					sourceTable.setTableName(object.getEtlStageObjectName());
					sourceTable.setColumns(srcColumns);
					sourceTable.setDataTypes(trgTypes);
					sourceTable.setDefaultColumns(new String[0]);
					sourceTable.setDefaultTypes(new String[0]);
					export.appendChild(sourceTable.getElement(document));
				
					// Generate and append target table
					SAPDSTable targetTable = new SAPDSTable();
					targetTable.setDataStore(sources.get(0).getEtlStageBodiDs());
					targetTable.setOwnerName(sources.get(0).getEtlStageOwner());
					targetTable.setTableName(object.getEtlStageStg1TableName());
					targetTable.setColumns(trgColumns);
					targetTable.setDataTypes(trgTypes);
					targetTable.setDefaultColumns(defaultColumns);
					targetTable.setDefaultTypes(defaultColTypes);
					export.appendChild(targetTable.getElement(document));
					
					// Generate and append dataflow
					SAPDSDataFlowBean dataFlow = new SAPDSDataFlowBean();
					dataFlow.setSourceDataStore(sources.get(0).getEtlStageSourceBodiDs());
					dataFlow.setTargetDataStore(sources.get(0).getEtlStageBodiDs());
					dataFlow.setTargetOwnerName(sources.get(0).getEtlStageOwner());
					dataFlow.setTargetTableName(object.getEtlStageStg1TableName());
					//
					dataFlow.setSourceColumns(srcColumns);
					dataFlow.setTargetColumns(trgColumns);
					dataFlow.setDataTypes(trgTypes);
					//
					dataFlow.setDefaultColumns(defaultColumns);
					dataFlow.setDefaultValues(defaultValues);
					dataFlow.setDefaultTypes(defaultColTypes);
					//
					dataFlow.setDataFlowName(dataFlowPrefix + "_" + object.getEtlStageStg2ViewName());
					dataFlow.setSourceOwnerName(sourceOwner);
					dataFlow.setSourceTableName(object.getEtlStageObjectName());
					export.appendChild(dataFlow.getElement(document));
					//exportParam.appendChild(targetTable.getElement(document));
					
					// For each distribution code
					/*for (StageSourceDb sourcedb:sourcedbs) {
						if (distributionCode == null  || distributionCode.equals("") || sourcedb.getEtlStageDistributionCode().equalsIgnoreCase(distributionCode)) {
							
							if (sourcedbs.size() > 1) {
								LOGGER.info("Object: " + object.getEtlStageObjectName() + " - " + sourcedb.getEtlStageDistributionCode());
								defaultValues[0] = "'" + sourcedb.getEtlStageDistributionCode() + "'";
							}
							
							// Determine identifier and source owner
							identifierRoot = object.getEtlStageStg2TableName();
							if (!(sourcedb.getEtlStageDistributionCode().equalsIgnoreCase("")) && sourcedb.getEtlStageDistributionCode() != null) {
								identifierRoot += "_" + sourcedb.getEtlStageDistributionCode();
							}
							if (sourcedb.getEtlStageSourceOwner().split(":").length>1) {
								sourceOwner = sourcedb.getEtlStageSourceOwner().split(":")[1];
							}
							else {
								sourceOwner = sourcedb.getEtlStageSourceOwner();
							}
							
							// Generate and append source table
							sourceTable.setDataStore(sourcedb.getEtlStageSourceBodiDs());
							sourceTable.setOwnerName(sourceOwner);
							sourceTable.setTableName(object.getEtlStageObjectName());
							sourceTable.setColumns(srcColumns);
							sourceTable.setDataTypes(trgTypes);
							sourceTable.setDefaultColumns(new String[0]);
							sourceTable.setDefaultTypes(new String[0]);
							export.appendChild(sourceTable.getElement(document));
	
							// Generate and append dataflow
							dataFlow.setDataFlowName(dataFlowPrefix + "_" + identifierRoot);
							dataFlow.setSourceDataStore(sourcedb.getEtlStageSourceBodiDs());
							dataFlow.setSourceOwnerName(sourceOwner);
							dataFlow.setSourceTableName(object.getEtlStageObjectName());
							dataFlow.setTargetDataStore(sources.get(0).getEtlStageBodiDs());
							dataFlow.setTargetOwnerName(sources.get(0).getEtlStageOwner());
							dataFlow.setTargetTableName(object.getEtlStageStg1TableName());
							//
							dataFlow.setSourceColumns(srcColumns);
							dataFlow.setTargetColumns(trgColumns);
							dataFlow.setDataTypes(trgTypes);
							//
							dataFlow.setDefaultColumns(defaultColumns);
							dataFlow.setDefaultValues(defaultValues);
							dataFlow.setDefaultTypes(defaultColTypes);
							//
							export.appendChild(dataFlow.getElement(document));
	
							// Generate and append job
							job.setDataFlowName(dataFlowPrefix + "_" + identifierRoot);
							job.setJobName(jobPrefix + "_" + identifierRoot);
							job.setStageSourceCode(sources.get(0).getEtlStageSourceCode());
							job.setStageObjectName(object.getEtlStageObjectName());
							export.appendChild(job.getElement(document));
						}
					}*/
				}
				// Generate and append workflow
				SAPDSWorkFlow workFlow = new SAPDSWorkFlow();
				workFlow.setDataFlowName(dataFlowPrefix + "_" + object.getEtlStageStg2ViewName());
				workFlow.setWorkFlowName(workFlowPrefix + "_" + object.getEtlStageStg2ViewName());
				export.appendChild(workFlow.getElement(document));
				// Generate and append job
				SAPDSJobBean job = new SAPDSJobBean();
				job.setWorkFlowName(workFlowPrefix + "_" + object.getEtlStageStg2ViewName());
				job.setJobName(jobPrefix + "_" + object.getEtlStageStg2ViewName());
				job.setStageSourceCode(sources.get(0).getEtlStageSourceCode());
				job.setStageObjectName(object.getEtlStageObjectName());
				export.appendChild(job.getElement(document));
				//}
				
				LOGGER.info("Source: " + stageSourceCode + " - Object: " + object.getEtlStageObjectName() + " - END");
			}
		}
		// write the content into file
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.transform(new DOMSource(export), new StreamResult(new File(exportFileName + ".xml")));
		//transformer.transform(new DOMSource(exportParam), new StreamResult(new File(exportFileName + "_param.xml")));
		LOGGER.info("Generated stage1 bodi objects for source: " + stageSourceCode);
    }

}
