CREATE OR REPLACE PACKAGE stg_ddl AUTHID CURRENT_USER
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-07-24 16:48:57 +0200 (Di, 24 Jul 2012) $
    * $Revision: 3027 $
    * $Id: stg_ddl-def.sql 3027 2012-07-24 14:48:57Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_ddl/stg_ddl-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version     CONSTANT VARCHAR2 (1024)           := '$Id: stg_ddl-def.sql 3027 2012-07-24 14:48:57Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url         CONSTANT VARCHAR2 (1024)           := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_ddl/stg_ddl-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version              VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                  VARCHAR2 (1024);
   -- Object related identifiers
   g_n_object_id               NUMBER;
   g_n_source_nk_flag          NUMBER;
   g_n_fbda_flag               NUMBER;
   g_n_parallel_degree         NUMBER;
   g_vc_source_code            type.vc_obj_plsql;
   g_vc_object_name            type.vc_obj_plsql;
   g_vc_prefix_src             type.vc_obj_plsql;
   g_vc_dblink                 type.vc_obj_plsql;
   g_vc_owner_src              type.vc_obj_plsql;
   g_vc_owner_stg              type.vc_obj_plsql;
   g_vc_table_comment          type.vc_max_plsql;
   g_vc_table_name_source      type.vc_obj_plsql;
   g_vc_table_name_diff        type.vc_obj_plsql;
   g_vc_table_name_dupl        type.vc_obj_plsql;
   g_vc_table_name_stage1      type.vc_obj_plsql;
   g_vc_table_name_stage2      type.vc_obj_plsql;
   g_vc_nk_name_diff           type.vc_obj_plsql;
   g_vc_nk_name_stage1         type.vc_obj_plsql;
   g_vc_nk_name_stage2         type.vc_obj_plsql;
   g_vc_view_name_stage2       type.vc_obj_plsql;
   g_vc_view_name_history      type.vc_obj_plsql;
   g_vc_package_main           type.vc_obj_plsql;
   g_vc_filter_clause          type.vc_max_plsql;
   g_vc_dedupl_rank_clause     type.vc_max_plsql;
   g_vc_partition_clause       type.vc_max_plsql;
   g_vc_increment_column       type.vc_max_plsql;
   g_vc_increment_coldef       type.vc_max_plsql;
   g_n_increment_buffer        NUMBER;
   --
   g_vc_tablespace_stg1_data   type.vc_obj_plsql;
   g_vc_tablespace_stg1_indx   type.vc_obj_plsql;
   g_vc_tablespace_stg2_data   type.vc_obj_plsql;
   g_vc_tablespace_stg2_indx   type.vc_obj_plsql;
   g_vc_fb_archive             type.vc_obj_plsql;
   -- List of source related identifiers
   g_l_dblink                  DBMS_SQL.varchar2s;
   g_l_owner_src               DBMS_SQL.varchar2s;
   g_l_distr_code              DBMS_SQL.varchar2s;
   -- List of columns
   g_vc_col_def                type.vc_max_plsql;
   g_vc_col_all                type.vc_max_plsql;
   g_vc_col_pk_src             type.vc_max_plsql;
   g_vc_col_pk                 type.vc_max_plsql;
   -- History => root features
   g_vc_table_name_hist        type.vc_obj_plsql;
   g_vc_col_hist_order         type.vc_max_plsql;

   PROCEDURE prc_create_stage1_table (
      p_b_drop_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_duplicate_table (
      p_b_drop_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_diff_table (
      p_b_drop_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_stage2_table (
      p_b_drop_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_stage2_view (
      p_b_raise_flag   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_stage2_synonym (
      p_b_raise_flag   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_stage2_hist (
      p_b_raise_flag   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_package_main (
      p_b_tc_only_flag   BOOLEAN DEFAULT FALSE
    , p_b_raise_flag     BOOLEAN DEFAULT FALSE
   );
END stg_ddl;
/

SHOW errors