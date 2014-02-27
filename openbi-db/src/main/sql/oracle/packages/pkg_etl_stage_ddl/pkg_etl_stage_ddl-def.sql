CREATE OR REPLACE PACKAGE pkg_etl_stage_ddl AUTHID CURRENT_USER
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-07-24 16:48:57 +0200 (Di, 24 Jul 2012) $
    * $Revision: 3027 $
    * $Id: pkg_etl_stage_ddl-def.sql 3027 2012-07-24 14:48:57Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_ddl/pkg_etl_stage_ddl-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version     CONSTANT VARCHAR2 (1024)           := '$Id: pkg_etl_stage_ddl-def.sql 3027 2012-07-24 14:48:57Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url         CONSTANT VARCHAR2 (1024)           := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_ddl/pkg_etl_stage_ddl-def.sql $';
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
   g_vc_source_code            pkg_utl_type.vc_obj_plsql;
   g_vc_object_name            pkg_utl_type.vc_obj_plsql;
   g_vc_prefix_src             pkg_utl_type.vc_obj_plsql;
   g_vc_dblink                 pkg_utl_type.vc_obj_plsql;
   g_vc_owner_src              pkg_utl_type.vc_obj_plsql;
   g_vc_owner_stg              pkg_utl_type.vc_obj_plsql;
   g_vc_table_comment          pkg_utl_type.vc_max_plsql;
   g_vc_table_name_source      pkg_utl_type.vc_obj_plsql;
   g_vc_table_name_diff        pkg_utl_type.vc_obj_plsql;
   g_vc_table_name_dupl        pkg_utl_type.vc_obj_plsql;
   g_vc_table_name_stage1      pkg_utl_type.vc_obj_plsql;
   g_vc_table_name_stage2      pkg_utl_type.vc_obj_plsql;
   g_vc_nk_name_diff           pkg_utl_type.vc_obj_plsql;
   g_vc_nk_name_stage1         pkg_utl_type.vc_obj_plsql;
   g_vc_nk_name_stage2         pkg_utl_type.vc_obj_plsql;
   g_vc_view_name_stage2       pkg_utl_type.vc_obj_plsql;
   g_vc_view_name_history      pkg_utl_type.vc_obj_plsql;
   g_vc_package_main           pkg_utl_type.vc_obj_plsql;
   g_vc_filter_clause          pkg_utl_type.vc_max_plsql;
   g_vc_dedupl_rank_clause     pkg_utl_type.vc_max_plsql;
   g_vc_partition_clause       pkg_utl_type.vc_max_plsql;
   g_vc_increment_column       pkg_utl_type.vc_max_plsql;
   g_vc_increment_coldef       pkg_utl_type.vc_max_plsql;
   g_n_increment_buffer        NUMBER;
   --
   g_vc_tablespace_stg1_data   pkg_utl_type.vc_obj_plsql;
   g_vc_tablespace_stg1_indx   pkg_utl_type.vc_obj_plsql;
   g_vc_tablespace_stg2_data   pkg_utl_type.vc_obj_plsql;
   g_vc_tablespace_stg2_indx   pkg_utl_type.vc_obj_plsql;
   g_vc_fb_archive             pkg_utl_type.vc_obj_plsql;
   -- List of source related identifiers
   g_l_dblink                  DBMS_SQL.varchar2s;
   g_l_owner_src               DBMS_SQL.varchar2s;
   g_l_distr_code              DBMS_SQL.varchar2s;
   -- List of columns
   g_vc_col_def                pkg_utl_type.vc_max_plsql;
   g_vc_col_all                pkg_utl_type.vc_max_plsql;
   g_vc_col_pk_src             pkg_utl_type.vc_max_plsql;
   g_vc_col_pk                 pkg_utl_type.vc_max_plsql;
   -- History => root features
   g_vc_table_name_hist        pkg_utl_type.vc_obj_plsql;
   g_vc_col_hist_order         pkg_utl_type.vc_max_plsql;

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
END pkg_etl_stage_ddl;
/

SHOW errors