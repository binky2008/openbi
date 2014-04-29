CREATE OR REPLACE PACKAGE stag_param
   AUTHID CURRENT_USER
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
   c_spec_version         CONSTANT VARCHAR2 (1024) := '$Id: $';
   /**
   * Package spec repository URL.
   */
   c_spec_url             CONSTANT VARCHAR2 (1024) := '$HeadURL: $';
   /**
   * Package body version string.
   */
   c_body_version                  VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                      VARCHAR2 (1024);
   /**
    * Default column names
    */
   c_vc_suffix_tab_source          VARCHAR2 (50) := 'SRC';
   c_vc_suffix_tab_stag            VARCHAR2 (50) := 'STG';
   c_vc_suffix_tab_hist            VARCHAR2 (50) := 'HST';
   c_vc_suffix_tab_diff            VARCHAR2 (50) := 'DIF';
   c_vc_suffix_tab_dupl            VARCHAR2 (50) := 'DUP';
   c_vc_suffix_nk_diff             VARCHAR2 (50) := 'DNK';
   c_vc_suffix_nk_hist             VARCHAR2 (50) := 'HNK';
   c_vc_suffix_view_fbda           TYPE.vc_max_plsql := 'H';
   c_vc_suffix_package             TYPE.vc_max_plsql := 'PKG';
   c_vc_prefix_partition           TYPE.vc_max_plsql := 'P';
   --
   c_vc_procedure_trunc_stage      TYPE.vc_max_plsql := 'prc_trunc_stage';
   c_vc_procedure_trunc_diff       TYPE.vc_max_plsql := 'prc_trunc_diff';
   c_vc_procedure_load_init        TYPE.vc_max_plsql := 'prc_load_init';
   c_vc_procedure_load_stage       TYPE.vc_max_plsql := 'prc_load_stage';
   c_vc_procedure_load_stage_p     TYPE.vc_max_plsql := 'prc_load_stage_p';
   c_vc_procedure_load_diff        TYPE.vc_max_plsql := 'prc_load_diff';
   c_vc_procedure_load_diff_incr   TYPE.vc_max_plsql := 'prc_load_diff_incr';
   c_vc_procedure_load_hist        TYPE.vc_max_plsql := 'prc_load_hist';
   c_vc_procedure_wrapper          TYPE.vc_max_plsql := 'prc_load';
   c_vc_procedure_wrapper_incr     TYPE.vc_max_plsql := 'prc_load_incr';
   --
   c_vc_column_stage_sk            VARCHAR2 (50) := 'DWH_SK';
   c_vc_column_valid_from          VARCHAR2 (50) := 'DWH_VALID_FROM';
   c_vc_column_valid_to            VARCHAR2 (50) := 'DWH_VALID_TO';
   c_vc_column_dml_op              VARCHAR2 (50) := 'DWH_OPERATION';
   c_vc_column_source_db           VARCHAR2 (50) := 'DWH_SOURCE_ID';
   c_vc_column_partition           VARCHAR2 (50) := 'DWH_PARTITION_ID';
   c_vc_column_system_src          VARCHAR2 (50) := 'DWH_SYSTEM';
   c_vc_column_active_version      VARCHAR2 (50) := 'DWH_ACTIVE';
   /**
    * Grantees
    */
   c_vc_list_grantee               VARCHAR2 (1000) := 'EDWH_CL,EDWH_QC,CORE';
END stag_param;