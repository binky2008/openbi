CREATE OR REPLACE PACKAGE p#frm#stag_ddl
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: stag_ddl-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_ddl/stag_ddl-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version      CONSTANT VARCHAR2 (1024) := '$Id: stag_ddl-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url          CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_ddl/stag_ddl-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version               VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                   VARCHAR2 (1024);

   /**
   * Object name type
   */
   SUBTYPE t_object_name IS VARCHAR2 (50);

   /**
   * String type
   */
   SUBTYPE t_string IS VARCHAR2 (32767);

   -- Object related identifiers
   g_n_object_id                NUMBER;
   g_n_source_nk_flag           NUMBER;
   g_n_fbda_flag                NUMBER;
   g_n_parallel_degree          NUMBER;
   g_vc_source_code             t_object_name;
   g_vc_object_name             t_object_name;
   g_vc_prefix_src              t_object_name;
   --
   g_vc_dblink                  t_object_name;
   g_vc_owner_src               t_object_name;
   g_vc_table_name_source       t_object_name;
   g_vc_source_identifier       t_object_name;
   --
   g_vc_owner_stg               t_object_name;
   g_vc_table_name_stage        t_object_name;
   g_vc_table_name_diff         t_object_name;
   g_vc_table_name_dupl         t_object_name;
   g_vc_table_name_hist         t_object_name;
   g_vc_table_comment           t_string;
   g_vc_nk_name_diff            t_object_name;
   g_vc_nk_name_stage           t_object_name;
   g_vc_nk_name_hist            t_object_name;
   g_vc_view_name_hist          t_object_name;
   g_vc_view_name_fbda          t_object_name;
   g_vc_package_main            t_object_name;
   g_vc_filter_clause           t_string;
   g_vc_dedupl_rank_clause      t_string;
   g_vc_partition_expr          t_string;
   g_vc_increment_column        t_string;
   g_vc_increment_coldef        t_string;
   g_n_increment_buffer         NUMBER;
   --
   g_vc_tablespace_stage_data   t_object_name;
   g_vc_tablespace_stage_indx   t_object_name;
   g_vc_tablespace_hist_data    t_object_name;
   g_vc_tablespace_hist_indx    t_object_name;
   g_vc_fb_archive              t_object_name;
   -- List of source related identifiers
   g_l_dblink                   DBMS_SQL.varchar2s;
   g_l_owner_src                DBMS_SQL.varchar2s;
   g_l_distr_code               DBMS_SQL.varchar2s;
   -- List of columns
   g_vc_col_def                 t_string;
   g_vc_col_all                 t_string;
   g_vc_col_pk_src              t_string;
   g_vc_col_pk                  t_string;
   --
   g_vc_col_hist                t_string;
   g_vc_col_update              t_string;

   PROCEDURE prc_create_stage_table (
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

   PROCEDURE prc_create_hist_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_create_hist_view (p_b_raise_flag BOOLEAN DEFAULT FALSE);

   PROCEDURE prc_create_hist_synonym (p_b_raise_flag BOOLEAN DEFAULT FALSE);

   PROCEDURE prc_create_fbda_view (p_b_raise_flag BOOLEAN DEFAULT FALSE);

   PROCEDURE prc_create_package_main (
      p_b_hist_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag        BOOLEAN DEFAULT FALSE
   );
END p#frm#stag_ddl;