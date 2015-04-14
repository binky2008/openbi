package org.openbusinessintelligence.meta.entity;

import java.io.*;

import javax.persistence.*;

import org.openbusinessintelligence.meta.entity.StageSourceDb;

import java.util.*;


/**
 * The persistent class for the ETL_STAGE_SOURCE_T database table.
 * 
 */
@Entity
@Table(name="ETL_STAGE_SOURCE_T")
public class StageSource implements Serializable {
	private static final long serialVersionUID = 1L;
	private long etlStageSourceId;
	private String etlStageOwner;
	private String etlStageSourceCode;
	private String etlStageSourceName;
	private String etlStageSourcePrefix;
	private String etlStageTsStg1Data;
	private String etlStageTsStg1Indx;
	private String etlStageTsStg2Data;
	private String etlStageTsStg2Indx;
	private String etlStageBodiDs;
	private String etlStageSourceBodiDs;
	private List<StageObject> stageObject;
	private List<StageSourceDb> stageSourceDb;
	public StageSource() {
    }


	@Id
	@SequenceGenerator(name="ETL_STAGE_SOURCE_SEQ", sequenceName="ETL_STAGE_SOURCE_SEQ")
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ETL_STAGE_SOURCE_SEQ")
	@Column(name="ETL_STAGE_SOURCE_ID", unique=false, nullable=false, precision=22)
	public long getEtlStageSourceId() {
		return this.etlStageSourceId;
	}

	public void setEtlStageSourceId(long etlStageSourceId) {
		this.etlStageSourceId = etlStageSourceId;
	}


	@Column(name="ETL_STAGE_OWNER", length=100)
	public String getEtlStageOwner() {
		return this.etlStageOwner;
	}

	public void setEtlStageOwner(String etlStageOwner) {
		this.etlStageOwner = etlStageOwner;
	}


	@Column(name="ETL_STAGE_SOURCE_CODE", length=10)
	public String getEtlStageSourceCode() {
		return this.etlStageSourceCode;
	}

	public void setEtlStageSourceCode(String etlStageSourceCode) {
		this.etlStageSourceCode = etlStageSourceCode;
	}


	@Column(name="ETL_STAGE_SOURCE_NAME", length=1000)
	public String getEtlStageSourceName() {
		return this.etlStageSourceName;
	}

	public void setEtlStageSourceName(String etlStageSourceName) {
		this.etlStageSourceName = etlStageSourceName;
	}


	@Column(name="ETL_STAGE_SOURCE_PREFIX", length=10)
	public String getEtlStageSourcePrefix() {
		return this.etlStageSourcePrefix;
	}

	public void setEtlStageSourcePrefix(String etlStageSourcePrefix) {
		this.etlStageSourcePrefix = etlStageSourcePrefix;
	}


	@Column(name="ETL_STAGE_TS_STG1_DATA", length=100)
	public String getEtlStageTsStg1Data() {
		return this.etlStageTsStg1Data;
	}

	public void setEtlStageTsStg1Data(String etlStageTsStg1Data) {
		this.etlStageTsStg1Data = etlStageTsStg1Data;
	}


	@Column(name="ETL_STAGE_TS_STG1_INDX", length=100)
	public String getEtlStageTsStg1Indx() {
		return this.etlStageTsStg1Indx;
	}

	public void setEtlStageTsStg1Indx(String etlStageTsStg1Indx) {
		this.etlStageTsStg1Indx = etlStageTsStg1Indx;
	}


	@Column(name="ETL_STAGE_TS_STG2_DATA", length=100)
	public String getEtlStageTsStg2Data() {
		return this.etlStageTsStg2Data;
	}

	public void setEtlStageTsStg2Data(String etlStageTsStg2Data) {
		this.etlStageTsStg2Data = etlStageTsStg2Data;
	}


	@Column(name="ETL_STAGE_TS_STG2_INDX", length=100)
	public String getEtlStageTsStg2Indx() {
		return this.etlStageTsStg2Indx;
	}

	public void setEtlStageTsStg2Indx(String etlStageTsStg2Indx) {
		this.etlStageTsStg2Indx = etlStageTsStg2Indx;
	}


	@Column(name="ETL_STAGE_BODI_DS", length=100)
	public String getEtlStageBodiDs() {
		return this.etlStageBodiDs;
	}

	public void setEtlStageBodiDs(String etlStageBodiDs) {
		this.etlStageBodiDs = etlStageBodiDs;
	}


	@Column(name="ETL_STAGE_SOURCE_BODI_DS", length=100)
	public String getEtlStageSourceBodiDs() {
		return this.etlStageSourceBodiDs;
	}

	public void setEtlStageSourceBodiDs(String etlStageSourceBodiDs) {
		this.etlStageSourceBodiDs = etlStageSourceBodiDs;
	}


	@OneToMany(mappedBy = "stageSource")
	public List<StageObject> getStageObject() {
	    return stageObject;
	}


	public void setStageObject(List<StageObject> param) {
	    this.stageObject = param;
	}


	@OneToMany(mappedBy = "stageSource")
	public List<StageSourceDb> getStageSourceDb() {
	    return stageSourceDb;
	}


	public void setStageSourceDb(List<StageSourceDb> param) {
	    this.stageSourceDb = param;
	}

}