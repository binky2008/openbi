CREATE OR REPLACE PACKAGE stag_ddl
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
   c_spec_version     CONSTANT VARCHAR2 (1024) := '$Id: $';
   /**
   * Package spec repository URL.
   */
   c_spec_url         CONSTANT VARCHAR2 (1024) := '$HeadURL: $';
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
   g_vc_source_code            TYPE.vc_obj_plsql;
   g_vc_object_name            TYPE.vc_obj_plsql;
   g_vc_prefix_src             TYPE.vc_obj_plsql;
   g_vc_dblink                 TYPE.vc_obj_plsql;
   g_vc_owner_src              TYPE.vc_obj_plsql;
   g_vc_owner_stg              TYPE.vc_obj_plsql;
   g_vc_table_comment          TYPE.vc_max_plsql;
   g_vc_table_name_source      TYPE.vc_obj_plsql;
   g_vc_table_name_diff        TYPE.vc_obj_plsql;
   g_vc_table_name_dupl        TYPE.vc_obj_plsql;
   g_vc_table_name_stage1      TYPE.vc_obj_plsql;
   g_vc_table_name_stage2      TYPE.vc_obj_plsql;
   g_vc_nk_name_diff           TYPE.vc_obj_plsql;
   g_vc_nk_name_stage1         TYPE.vc_obj_plsql;
   g_vc_nk_name_stage2         TYPE.vc_obj_plsql;
   g_vc_view_name_stage2       TYPE.vc_obj_plsql;
   g_vc_view_name_history      TYPE.vc_obj_plsql;
   g_vc_package_main           TYPE.vc_obj_plsql;
   g_vc_filter_clause          TYPE.vc_max_plsql;
   g_vc_dedupl_rank_clause     TYPE.vc_max_plsql;
   g_vc_partition_clause       TYPE.vc_max_plsql;
   g_vc_increment_column       TYPE.vc_max_plsql;
   g_vc_increment_coldef       TYPE.vc_max_plsql;
   g_n_increment_buffer        NUMBER;
   --
   g_vc_tablespace_stg1_data   TYPE.vc_obj_plsql;
   g_vc_tablespace_stg1_indx   TYPE.vc_obj_plsql;
   g_vc_tablespace_stg2_data   TYPE.vc_obj_plsql;
   g_vc_tablespace_stg2_indx   TYPE.vc_obj_plsql;
   g_vc_fb_archive             TYPE.vc_obj_plsql;
   -- List of source related identifiers
   g_l_dblink                  DBMS_SQL.varchar2s;
   g_l_owner_src               DBMS_SQL.varchar2s;
   g_l_distr_code              DBMS_SQL.varchar2s;
   -- List of columns
   g_vc_col_def                TYPE.vc_max_plsql;
   g_vc_col_all                TYPE.vc_max_plsql;
   g_vc_col_pk_src             TYPE.vc_max_plsql;
   g_vc_col_pk                 TYPE.vc_max_plsql;
   -- History => root features
   g_vc_table_name_hist        TYPE.vc_obj_plsql;
   g_vc_col_hist_order         TYPE.vc_max_plsql;

   PROCEDURE prc_create_stage1_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_duplicate_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_diff_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_stage2_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_stage2_view (p_b_raise_flag BOOLEAN DEFAULT FALSE);

   PROCEDURE prc_create_stage2_synonym (p_b_raise_flag BOOLEAN DEFAULT FALSE);

   PROCEDURE prc_create_stage2_hist (p_b_raise_flag BOOLEAN DEFAULT FALSE);

   PROCEDURE prc_create_package_main (
      p_b_tc_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   );
END stag_ddl;