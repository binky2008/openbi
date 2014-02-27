CREATE OR REPLACE PACKAGE pkg_etl_stage_param AUTHID CURRENT_USER
AS
   /**
   * $Author: nmarangoni $
   * $Date: 2012-05-15 10:37:50 +0200 (Di, 15 Mai 2012) $
   * $Revision: 2783 $
   * $Id: pkg_etl_stage_param-def.sql 2783 2012-05-15 08:37:50Z nmarangoni $
   * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_param/pkg_etl_stage_param-def.sql $
   */
   /**
    * Package spec version string.
    */
   c_spec_version      CONSTANT VARCHAR2 (1024) := '$Id: pkg_etl_stage_param-def.sql 2783 2012-05-15 08:37:50Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url          CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_param/pkg_etl_stage_param-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version               VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                   VARCHAR2 (1024);
   /**
    * Default column names
    */
   c_vc_column_exec_id_upd      VARCHAR2 (50)   := 'DI_GUI';
   c_vc_column_exec_id_ins      VARCHAR2 (50)   := 'DI_GUI_INS';
   c_vc_column_timestamp        VARCHAR2 (50)   := 'DI_COMMIT_DT';
   c_vc_column_dml_op           VARCHAR2 (50)   := 'DI_OPERATION';
   c_vc_column_source_distr     VARCHAR2 (50)   := 'DI_REGION_ID';
   c_vc_column_partition        VARCHAR2 (50)   := 'DI_PARTITION_ID';
   c_vc_column_system_src       VARCHAR2 (1024) := 'DI_SYSTEM';
   c_vc_column_active_version   VARCHAR2 (1024) := 'ACTIVE';
   c_vc_column_valid_from       VARCHAR2 (1024) := 'VALID_FROM';
   c_vc_column_valid_to         VARCHAR2 (1024) := 'VALID_TO';
   /**
    * Grantees
    */
   c_vc_list_grantee            VARCHAR2 (1000) := 'EDWH_CL,EDWH_QC,CORE';
END pkg_etl_stage_param;
/

SHOW errors

BEGIN
   pkg_utl_ddl.prc_create_synonym ('pkg_etl_stage_param'
                                 , 'pkg_etl_stage_param'
                                 , TRUE
                                  );
END;
/

SHOW errors