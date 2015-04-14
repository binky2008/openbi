package org.openbusinessintelligence.meta.entity;

import java.io.Serializable;
import javax.persistence.*;
import java.math.BigDecimal;


/**
 * The persistent class for the ETL_STAGE_COLUMN_T database table.
 * 
 */
@Entity
@Table(name="ETL_STAGE_COLUMN_T")
public class StageColumn implements Serializable {
	private static final long serialVersionUID = 1L;
	private long etlStageColumnId;
	private String etlStageColumnComment;
	private String etlStageColumnDef;
	private String etlStageColumnDefSrc;
	private BigDecimal etlStageColumnIncrFlag;
	private BigDecimal etlStageColumnEdwhFlag;
	private String etlStageColumnName;
	private String etlStageColumnNameMap;
	private BigDecimal etlStageColumnNkPos;
	private BigDecimal etlStageColumnPos;
	private BigDecimal etlStageObjectId;

    public StageColumn() {
    }


	@Id
	@SequenceGenerator(name="ETL_STAGE_COLUMN_SEQ", sequenceName="ETL_STAGE_COLUMN_SEQ", allocationSize=1)
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ETL_STAGE_COLUMN_SEQ")
	@Column(name="ETL_STAGE_COLUMN_ID", unique=false, nullable=false, precision=22)
	public long getEtlStageColumnId() {
		return this.etlStageColumnId;
	}

	public void setEtlStageColumnId(long etlStageColumnId) {
		this.etlStageColumnId = etlStageColumnId;
	}


	@Column(name="ETL_STAGE_COLUMN_COMMENT", length=4000)
	public String getEtlStageColumnComment() {
		return this.etlStageColumnComment;
	}

	public void setEtlStageColumnComment(String etlStageColumnComment) {
		this.etlStageColumnComment = etlStageColumnComment;
	}


	@Column(name="ETL_STAGE_COLUMN_DEF", length=100)
	public String getEtlStageColumnDef() {
		return this.etlStageColumnDef;
	}

	public void setEtlStageColumnDef(String etlStageColumnDef) {
		this.etlStageColumnDef = etlStageColumnDef;
	}


	@Column(name="ETL_STAGE_COLUMN_DEF_SRC", length=100)
	public String getEtlStageColumnDefSrc() {
		return this.etlStageColumnDefSrc;
	}

	public void setEtlStageColumnDefSrc(String etlStageColumnDefSrc) {
		this.etlStageColumnDefSrc = etlStageColumnDefSrc;
	}


	@Column(name="ETL_STAGE_COLUMN_EDWH_FLAG", precision=22)
	public BigDecimal getEtlStageColumnEdwhFlag() {
		return this.etlStageColumnEdwhFlag;
	}

	public void setEtlStageColumnEdwhFlag(BigDecimal etlStageColumnEdwhFlag) {
		this.etlStageColumnEdwhFlag = etlStageColumnEdwhFlag;
	}


	@Column(name="ETL_STAGE_COLUMN_INCR_FLAG", precision=22)
	public BigDecimal getEtlStageColumnIncrFlag() {
		return this.etlStageColumnIncrFlag;
	}

	public void setEtlStageColumnIncrFlag(BigDecimal etlStageColumnIncrFlag) {
		this.etlStageColumnIncrFlag = etlStageColumnIncrFlag;
	}


	@Column(name="ETL_STAGE_COLUMN_NAME", length=100)
	public String getEtlStageColumnName() {
		return this.etlStageColumnName;
	}

	public void setEtlStageColumnName(String etlStageColumnName) {
		this.etlStageColumnName = etlStageColumnName;
	}


	@Column(name="ETL_STAGE_COLUMN_NAME_MAP", length=100)
	public String getEtlStageColumnNameMap() {
		return this.etlStageColumnNameMap;
	}

	public void setEtlStageColumnNameMap(String etlStageColumnNameMap) {
		this.etlStageColumnNameMap = etlStageColumnNameMap;
	}


	@Column(name="ETL_STAGE_COLUMN_NK_POS", precision=22)
	public BigDecimal getEtlStageColumnNkPos() {
		return this.etlStageColumnNkPos;
	}

	public void setEtlStageColumnNkPos(BigDecimal etlStageColumnNkPos) {
		this.etlStageColumnNkPos = etlStageColumnNkPos;
	}


	@Column(name="ETL_STAGE_COLUMN_POS", precision=22)
	public BigDecimal getEtlStageColumnPos() {
		return this.etlStageColumnPos;
	}

	public void setEtlStageColumnPos(BigDecimal etlStageColumnPos) {
		this.etlStageColumnPos = etlStageColumnPos;
	}


	@Column(name="ETL_STAGE_OBJECT_ID", precision=22)
	public BigDecimal getEtlStageObjectId() {
		return this.etlStageObjectId;
	}

	public void setEtlStageObjectId(BigDecimal etlStageObjectId) {
		this.etlStageObjectId = etlStageObjectId;
	}
}