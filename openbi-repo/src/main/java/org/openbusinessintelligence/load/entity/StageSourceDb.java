package org.openbusinessintelligence.load.entity;

import java.io.Serializable;
import javax.persistence.*;
import java.math.BigDecimal;

import org.openbusinessintelligence.load.entity.StageSource;


/**
 * The persistent class for the ETL_STAGE_SOURCE_DB_T database table.
 * 
 */
@Entity
@Table(name="ETL_STAGE_SOURCE_DB_T")
public class StageSourceDb implements Serializable {
	private static final long serialVersionUID = 1L;
	private long etlStageSourceDbId;
	private BigDecimal etlStageSourceId;
	private String etlStageDistributionCode;
	private String etlStageSourceDbLink;
	private String etlStageSourceDbJdbcname;
	private String etlStageSourceOwner;
	private String etlStageSourceBodiDs;
	
	private StageSource stageSource;

    public StageSourceDb() {
    }


	@Id
	@SequenceGenerator(name="ETL_STAGE_SOURCE_DB_SEQ", sequenceName="ETL_STAGE_SOURCE_DB_SEQ")
	@GeneratedValue(strategy=GenerationType.SEQUENCE, generator="ETL_STAGE_SOURCE_DB_SEQ")
	@Column(name="ETL_STAGE_SOURCE_DB_ID", unique=false, nullable=false, precision=22)
	public long getEtlStageSourceDbId() {
		return this.etlStageSourceDbId;
	}

	public void setEtlStageSourceDbId(long etlStageSourceDbId) {
		this.etlStageSourceDbId = etlStageSourceDbId;
	}
	

	@Column(name="ETL_STAGE_DISTRIBUTION_CODE", length=10)
	public String getEtlStageDistributionCode() {
		return this.etlStageDistributionCode;
	}

	public void setEtlStageDistributionCode(String etlStageDistributionCode) {
		this.etlStageDistributionCode = etlStageDistributionCode;
	}


	@Column(name="ETL_STAGE_SOURCE_DB_LINK", length=100)
	public String getEtlStageSourceDbLink() {
		return this.etlStageSourceDbLink;
	}

	public void setEtlStageSourceDbLink(String etlStageSourceDbLink) {
		this.etlStageSourceDbLink = etlStageSourceDbLink;
	}


	@Column(name="ETL_STAGE_SOURCE_DB_JDBCNAME", length=100)
	public String getEtlStageSourceDbJdbcname() {
		return this.etlStageSourceDbJdbcname;
	}

	public void setEtlStageSourceDbJdbcname(String etlStageSourceDbJdbcname) {
		this.etlStageSourceDbJdbcname = etlStageSourceDbJdbcname;
	}


	@Column(name="ETL_STAGE_SOURCE_OWNER", length=100)
	public String getEtlStageSourceOwner() {
		return this.etlStageSourceOwner;
	}

	public void setEtlStageSourceOwner(String etlStageSourceOwner) {
		this.etlStageSourceOwner = etlStageSourceOwner;
	}


	@Column(name="ETL_STAGE_SOURCE_ID", precision=22, insertable=false, updatable=false)
	public BigDecimal getEtlStageSourceId() {
		return this.etlStageSourceId;
	}

	public void setEtlStageSourceId(BigDecimal etlStageSourceId) {
		this.etlStageSourceId = etlStageSourceId;
	}


	@Column(name="ETL_STAGE_SOURCE_BODI_DS", length=100)
	public String getEtlStageSourceBodiDs() {
		return this.etlStageSourceBodiDs;
	}

	public void setEtlStageSourceBodiDs(String etlStageSourceBodiDs) {
		this.etlStageSourceBodiDs = etlStageSourceBodiDs;
	}



	@ManyToOne
	@JoinColumn(name = "ETL_STAGE_SOURCE_ID", referencedColumnName = "ETL_STAGE_SOURCE_ID")
	public StageSource getStageSource() {
	    return stageSource;
	}


	public void setStageSource(StageSource param) {
	    this.stageSource = param;
	}

}
