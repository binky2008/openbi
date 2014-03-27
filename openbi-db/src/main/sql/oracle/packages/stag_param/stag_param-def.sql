CREATE OR REPLACE PACKAGE stag_param AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);
   /**
    * Default column names
    */
   c_vc_column_stage_sk         VARCHAR2 (50)   := 'DWH_SK';
   c_vc_column_timestamp        VARCHAR2 (50)   := 'DWH_COMMIT_DT';
   c_vc_column_dml_op           VARCHAR2 (50)   := 'DWH_OPERATION';
   c_vc_column_source_distr     VARCHAR2 (50)   := 'DWH_SOURCE_ID';
   c_vc_column_partition        VARCHAR2 (50)   := 'DWH_PARTITION_ID';
   c_vc_column_system_src       VARCHAR2 (50)   := 'DWH_SYSTEM';
   c_vc_column_active_version   VARCHAR2 (50)   := 'DWH_ACTIVE';
   c_vc_column_valid_from       VARCHAR2 (50)   := 'DWH_VALID_FROM';
   c_vc_column_valid_to         VARCHAR2 (50)   := 'DWH_VALID_TO';
   /**
    * Grantees
    */
   c_vc_list_grantee            VARCHAR2 (1000) := 'EDWH_CL,EDWH_QC,CORE';
END stag_param;
/

SHOW errors