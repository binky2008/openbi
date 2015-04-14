package org.openbusinessintelligence.meta.entity;

import java.io.Serializable;
import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.ManyToOne;

import org.openbusinessintelligence.meta.entity.StageObject;
import org.openbusinessintelligence.meta.entity.StageSource;

import javax.persistence.JoinColumn;


/**
 * The persistent class for the ETL_STAGE_OBJECT_T database table.
 * 
 */
@Entity
@Table(name="ETL_STAGE_OBJECT_T")
public class StageObject implements Serializable {
	private static final long serialVersionUID = 1L;
	private long etlStageObjectId;
	private BigDecimal etlStageSourceId;
	private BigDecimal etlStageDeltaFlag;
	private String etlStageDiffNkName;
	private String etlStageDiffTableName;
	private String etlStageDuplTableName;
	private String etlStageFilterClause;
	private BigDecimal etlStageIncrementBuffer;
	private String etlStageObjectComment;
	private String etlStageObjectName;
	private String etlStageObjectRoot;
	private String etlStagePackageName;
	private BigDecimal etlStageParallelDegree;
	private String etlStagePartitionClause;
	private BigDecimal etlStageSourceNkFlag;
	private String etlStageSrcTableName;
	private String etlStageStg1TableName;
	private String etlStageStg2NkName;
	private String etlStageStg2TableName;
	private String etlStageStg2ViewName;
	private StageSource stageSource;
	public StageObject() {
    }


	@Id
	@SequenceGenerator(name="ETL_STAGE_OBJECT_SEQ", sequenceName="ETL_STAGE_OBJECT_SEQ")
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ETL_STAGE_OBJECT_SEQ")
	@Column(name="ETL_STAGE_OBJECT_ID", unique=false, nullable=false, precision=22)
	public long getEtlStageObjectId() {
		return this.etlStageObjectId;
	}

	public void setEtlStageObjectId(long etlStageObjectId) {
		this.etlStageObjectId = etlStageObjectId;
	}


	@Column(name="ETL_STAGE_DELTA_FLAG", precision=22)
	public BigDecimal getEtlStageDeltaFlag() {
		return this.etlStageDeltaFlag;
	}

	public void setEtlStageDeltaFlag(BigDecimal etlStageDeltaFlag) {
		this.etlStageDeltaFlag = etlStageDeltaFlag;
	}


	@Column(name="ETL_STAGE_DIFF_NK_NAME", length=100)
	public String getEtlStageDiffNkName() {
		return this.etlStageDiffNkName;
	}

	public void setEtlStageDiffNkName(String etlStageDiffNkName) {
		this.etlStageDiffNkName = etlStageDiffNkName;
	}


	@Column(name="ETL_STAGE_DIFF_TABLE_NAME", length=100)
	public String getEtlStageDiffTableName() {
		return this.etlStageDiffTableName;
	}

	public void setEtlStageDiffTableName(String etlStageDiffTableName) {
		this.etlStageDiffTableName = etlStageDiffTableName;
	}


	@Column(name="ETL_STAGE_DUPL_TABLE_NAME", length=100)
	public String getEtlStageDuplTableName() {
		return this.etlStageDuplTableName;
	}

	public void setEtlStageDuplTableName(String etlStageDuplTableName) {
		this.etlStageDuplTableName = etlStageDuplTableName;
	}


	@Column(name="ETL_STAGE_FILTER_CLAUSE", length=4000)
	public String getEtlStageFilterClause() {
		return this.etlStageFilterClause;
	}

	public void setEtlStageFilterClause(String etlStageFilterClause) {
		this.etlStageFilterClause = etlStageFilterClause;
	}


	@Column(name="ETL_STAGE_INCREMENT_BUFFER", precision=22)
	public BigDecimal getEtlStageIncrementBuffer() {
		return this.etlStageIncrementBuffer;
	}

	public void setEtlStageIncrementBuffer(BigDecimal etlStageIncrementBuffer) {
		this.etlStageIncrementBuffer = etlStageIncrementBuffer;
	}



	@Column(name="ETL_STAGE_OBJECT_COMMENT", length=4000)
	public String getEtlStageObjectComment() {
		return this.etlStageObjectComment;
	}

	public void setEtlStageObjectComment(String etlStageObjectComment) {
		this.etlStageObjectComment = etlStageObjectComment;
	}


	@Column(name="ETL_STAGE_OBJECT_NAME", length=100)
	public String getEtlStageObjectName() {
		return this.etlStageObjectName;
	}

	public void setEtlStageObjectName(String etlStageObjectName) {
		this.etlStageObjectName = etlStageObjectName;
	}


	@Column(name="ETL_STAGE_OBJECT_ROOT", length=100)
	public String getEtlStageObjectRoot() {
		return this.etlStageObjectRoot;
	}

	public void setEtlStageObjectRoot(String etlStageObjectRoot) {
		this.etlStageObjectRoot = etlStageObjectRoot;
	}


	@Column(name="ETL_STAGE_PACKAGE_NAME", length=100)
	public String getEtlStagePackageName() {
		return this.etlStagePackageName;
	}

	public void setEtlStagePackageName(String etlStagePackageName) {
		this.etlStagePackageName = etlStagePackageName;
	}


	@Column(name="ETL_STAGE_PARALLEL_DEGREE", precision=22)
	public BigDecimal getEtlStageParallelDegree() {
		return this.etlStageParallelDegree;
	}

	public void setEtlStageParallelDegree(BigDecimal etlStageParallelDegree) {
		this.etlStageParallelDegree = etlStageParallelDegree;
	}


	@Column(name="ETL_STAGE_PARTITION_CLAUSE", length=4000)
	public String getEtlStagePartitionClause() {
		return this.etlStagePartitionClause;
	}

	public void setEtlStagePartitionClause(String etlStagePartitionClause) {
		this.etlStagePartitionClause = etlStagePartitionClause;
	}
	

	@Column(name="ETL_STAGE_SOURCE_NK_FLAG", precision=22)
	public BigDecimal getEtlStageSourceNkFlag() {
		return this.etlStageSourceNkFlag;
	}

	public void setEtlStageSourceNkFlag(BigDecimal etlStageSourceNkFlag) {
		this.etlStageSourceNkFlag = etlStageSourceNkFlag;
	}


	@Column(name="ETL_STAGE_SRC_TABLE_NAME", length=100)
	public String getEtlStageSrcTableName() {
		return this.etlStageSrcTableName;
	}

	public void setEtlStageSrcTableName(String etlStageSrcTableName) {
		this.etlStageSrcTableName = etlStageSrcTableName;
	}


	@Column(name="ETL_STAGE_STG1_TABLE_NAME", length=100)
	public String getEtlStageStg1TableName() {
		return this.etlStageStg1TableName;
	}

	public void setEtlStageStg1TableName(String etlStageStg1TableName) {
		this.etlStageStg1TableName = etlStageStg1TableName;
	}


	@Column(name="ETL_STAGE_STG2_NK_NAME", length=100)
	public String getEtlStageStg2NkName() {
		return this.etlStageStg2NkName;
	}

	public void setEtlStageStg2NkName(String etlStageStg2NkName) {
		this.etlStageStg2NkName = etlStageStg2NkName;
	}


	@Column(name="ETL_STAGE_STG2_TABLE_NAME", length=100)
	public String getEtlStageStg2TableName() {
		return this.etlStageStg2TableName;
	}

	public void setEtlStageStg2TableName(String etlStageStg2TableName) {
		this.etlStageStg2TableName = etlStageStg2TableName;
	}


	@Column(name="ETL_STAGE_STG2_VIEW_NAME", length=100)
	public String getEtlStageStg2ViewName() {
		return this.etlStageStg2ViewName;
	}

	public void setEtlStageStg2ViewName(String etlStageStg2ViewName) {
		this.etlStageStg2ViewName = etlStageStg2ViewName;
	}


	@Column(name="ETL_STAGE_SOURCE_ID", precision=22, insertable=false, updatable=false)
	public BigDecimal getEtlStageSourceId() {
		return this.etlStageSourceId;
	}

	public void setEtlStageSourceId(BigDecimal etlStageSourceId) {
		this.etlStageSourceId = etlStageSourceId;
	}


	@ManyToOne
	@JoinColumn(name = "ETL_STAGE_SOURCE_ID", referencedColumnName = "ETL_STAGE_SOURCE_ID", nullable = false)
	public StageSource getStageSource() {
	    return stageSource;
	}


	public void setStageSource(StageSource param) {
	    this.stageSource = param;
	}

}