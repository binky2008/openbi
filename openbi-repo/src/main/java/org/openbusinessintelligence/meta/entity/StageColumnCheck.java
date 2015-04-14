package org.openbusinessintelligence.meta.entity;

import java.io.Serializable;
import javax.persistence.*;
import java.math.BigDecimal;


/**
 * The persistent class for the ETL_STAGE_COLUMN_CHECK_T database table.
 * 
 */
@Entity
@Table(name="ETL_STAGE_COLUMN_CHECK_T")
public class StageColumnCheck implements Serializable {
	private static final long serialVersionUID = 1L;
	private long etlStageColumnCheckId;
	private String etlStageColumnDef;
	private String etlStageColumnName;
	private BigDecimal etlStageColumnNkPos;
	private BigDecimal etlStageColumnPos;
	private BigDecimal etlStageObjectId;

    public StageColumnCheck() {
    }
    
	@Id
	@SequenceGenerator(name="ETL_STAGE_COLUMN_CHECK_SEQ", sequenceName="ETL_STAGE_COLUMN_CHECK_SEQ", allocationSize=1)
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ETL_STAGE_COLUMN_CHECK_SEQ")
	@Column(name="ETL_STAGE_COLUMN_CHECK_ID", unique=false, nullable=false, precision=22)
	public long getEtlStageColumnCheckId() {
		return this.etlStageColumnCheckId;
	}

	public void setEtlStageColumnCheckId(long etlStageColumnCheckId) {
		this.etlStageColumnCheckId = etlStageColumnCheckId;
	}
	

	@Column(name="ETL_STAGE_COLUMN_DEF", length=100)
	public String getEtlStageColumnDef() {
		return this.etlStageColumnDef;
	}

	public void setEtlStageColumnDef(String etlStageColumnDef) {
		this.etlStageColumnDef = etlStageColumnDef;
	}


	@Column(name="ETL_STAGE_COLUMN_NAME", length=100)
	public String getEtlStageColumnName() {
		return this.etlStageColumnName;
	}

	public void setEtlStageColumnName(String etlStageColumnName) {
		this.etlStageColumnName = etlStageColumnName;
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