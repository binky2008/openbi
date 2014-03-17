CREATE OR REPLACE PACKAGE stg_param AUTHID CURRENT_USER
AS
   /**
   * $Author: nmarangoni $
   * $Date: 2012-05-15 10:37:50 +0200 (Di, 15 Mai 2012) $
   * $Revision: 2783 $
   * $Id: stg_param-def.sql 2783 2012-05-15 08:37:50Z nmarangoni $
   * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_param/stg_param-def.sql $
   */
   /**
    * Package spec version string.
    */
   c_spec_version      CONSTANT VARCHAR2 (1024) := '$Id: stg_param-def.sql 2783 2012-05-15 08:37:50Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url          CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_param/stg_param-def.sql $';
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
   c_vc_column_exec_id_upd      VARCHAR2 (50)   := 'DWH_GUI';
   c_vc_column_exec_id_ins      VARCHAR2 (50)   := 'DWH_GUI_INS';
   c_vc_column_timestamp        VARCHAR2 (50)   := 'DWH_COMMIT_DT';
   c_vc_column_dml_op           VARCHAR2 (50)   := 'DWH_OPERATION';
   c_vc_column_source_distr     VARCHAR2 (50)   := 'DWH_SOURCE_ID';
   c_vc_column_partition        VARCHAR2 (50)   := 'DWH_PARTITION_ID';
   c_vc_column_system_src       VARCHAR2 (50)   := 'DWH_SYSTEM';
   c_vc_column_active_version   VARCHAR2 (50)   := 'ACTIVE';
   c_vc_column_valid_from       VARCHAR2 (50)   := 'VALID_FROM';
   c_vc_column_valid_to         VARCHAR2 (50)   := 'VALID_TO';
   /**
    * Grantees
    */
   c_vc_list_grantee            VARCHAR2 (1000) := 'EDWH_CL,EDWH_QC,CORE';
END stg_param;
/

SHOW errors

BEGIN
   ddl.prc_create_synonym ('stg_param'
                                 , 'stg_param'
                                 , TRUE
                                  );
END;
/

SHOW errors