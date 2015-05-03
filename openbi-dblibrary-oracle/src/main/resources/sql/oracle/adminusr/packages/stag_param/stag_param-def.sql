CREATE OR REPLACE PACKAGE p#frm#stag_param
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: stag_param-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_param/stag_param-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version         CONSTANT VARCHAR2 (1024) := '$Id: stag_param-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url             CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_param/stag_param-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version                  VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                      VARCHAR2 (1024);

   /**
   * Object name type
   */
   SUBTYPE t_object_name IS VARCHAR2 (50);

   /**
   * String type
   */
   SUBTYPE t_string IS VARCHAR2 (32767);

   /**
    * Default namesand prefixes
    */
   c_vc_suffix_tab_source          t_object_name := 'SRC';
   c_vc_suffix_tab_stag            t_object_name := 'STG';
   c_vc_suffix_tab_hist            t_object_name := 'HST';
   c_vc_suffix_tab_diff            t_object_name := 'DIF';
   c_vc_suffix_tab_dupl            t_object_name := 'DUP';
   c_vc_suffix_nk_diff             t_object_name := 'DNK';
   c_vc_suffix_nk_hist             t_object_name := 'HNK';
   c_vc_suffix_view_fbda           t_object_name := 'H';
   c_vc_suffix_package             t_object_name := 'PKG';
   c_vc_prefix_partition           t_object_name := 'P';
   --
   c_vc_procedure_trunc_stage      t_object_name := 'prc_trunc_stage';
   c_vc_procedure_trunc_diff       t_object_name := 'prc_trunc_diff';
   c_vc_procedure_load_init        t_object_name := 'prc_load_init';
   c_vc_procedure_load_stage       t_object_name := 'prc_load_stage';
   c_vc_procedure_load_stage_p     t_object_name := 'prc_load_stage_p';
   c_vc_procedure_load_diff        t_object_name := 'prc_load_diff';
   c_vc_procedure_load_diff_incr   t_object_name := 'prc_load_diff_incr';
   c_vc_procedure_load_hist        t_object_name := 'prc_load_hist';
   c_vc_procedure_wrapper          t_object_name := 'prc_load';
   c_vc_procedure_wrapper_incr     t_object_name := 'prc_load_incr';
   --
   c_vc_column_stage_sk            t_object_name := 'DWH_SK';
   c_vc_column_valid_from          t_object_name := 'DWH_VALID_FROM';
   c_vc_column_valid_to            t_object_name := 'DWH_VALID_TO';
   c_vc_column_dml_op              t_object_name := 'DWH_OPERATION';
   c_vc_column_source_db           t_object_name := 'DWH_SOURCE_ID';
   c_vc_column_partition           t_object_name := 'DWH_PARTITION_ID';
   c_vc_column_system_src          t_object_name := 'DWH_SYSTEM';
   c_vc_column_active_version      t_object_name := 'DWH_ACTIVE';
   /**
    * Grantees
    */
   c_vc_list_grantee               t_string := 'DWHCORE';
END p#frm#stag_param;