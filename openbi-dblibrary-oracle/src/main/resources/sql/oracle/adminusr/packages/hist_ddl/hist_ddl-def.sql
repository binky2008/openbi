CREATE OR REPLACE PACKAGE p#frm#hist_ddl
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: hist_ddl-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/hist_ddl/hist_ddl-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version     CONSTANT VARCHAR2 (1024) := '$Id: hist_ddl-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url         CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/hist_ddl/hist_ddl-def.sql $';
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
   g_vc_source_code            p#frm#type.vc_object_name;
   g_vc_object_name            p#frm#type.vc_object_name;
   g_vc_prefix_src             p#frm#type.vc_object_name;
   g_vc_dblink                 p#frm#type.vc_object_name;
   g_vc_owner_src              p#frm#type.vc_object_name;
   g_vc_owner_stg              p#frm#type.vc_object_name;
   g_vc_table_comment          p#frm#type.vc_max_plsql;
   g_vc_table_name_source      p#frm#type.vc_object_name;
   g_vc_table_name_diff        p#frm#type.vc_object_name;
   g_vc_table_name_dupl        p#frm#type.vc_object_name;
   g_vc_table_name_stage1      p#frm#type.vc_object_name;
   g_vc_table_name_stage2      p#frm#type.vc_object_name;
   g_vc_nk_name_diff           p#frm#type.vc_object_name;
   g_vc_nk_name_stage1         p#frm#type.vc_object_name;
   g_vc_nk_name_stage2         p#frm#type.vc_object_name;
   g_vc_view_name_stage2       p#frm#type.vc_object_name;
   g_vc_view_name_history      p#frm#type.vc_object_name;
   g_vc_package_main           p#frm#type.vc_object_name;
   g_vc_filter_clause          p#frm#type.vc_max_plsql;
   g_vc_dedupl_rank_clause     p#frm#type.vc_max_plsql;
   g_vc_partition_clause       p#frm#type.vc_max_plsql;
   g_vc_increment_column       p#frm#type.vc_max_plsql;
   g_vc_increment_coldef       p#frm#type.vc_max_plsql;
   g_n_increment_buffer        NUMBER;
   --
   g_vc_tablespace_stg1_data   p#frm#type.vc_object_name;
   g_vc_tablespace_stg1_indx   p#frm#type.vc_object_name;
   g_vc_tablespace_stg2_data   p#frm#type.vc_object_name;
   g_vc_tablespace_stg2_indx   p#frm#type.vc_object_name;
   g_vc_fb_archive             p#frm#type.vc_object_name;
   -- List of source related identifiers
   g_l_dblink                  DBMS_SQL.varchar2s;
   g_l_owner_src               DBMS_SQL.varchar2s;
   g_l_distr_code              DBMS_SQL.varchar2s;
   -- List of columns
   g_vc_col_def                p#frm#type.vc_max_plsql;
   g_vc_col_all                p#frm#type.vc_max_plsql;
   g_vc_col_pk_src             p#frm#type.vc_max_plsql;
   g_vc_col_pk                 p#frm#type.vc_max_plsql;
   -- History => root features
   g_vc_table_name_hist        p#frm#type.vc_object_name;
   g_vc_col_hist_order         p#frm#type.vc_max_plsql;

   FUNCTION fct_get_identifier (
      p_vc_dblink         VARCHAR2
    , p_vc_schema_name    VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
      RETURN VARCHAR2;

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
      p_b_tc_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   );
END p#frm#hist_ddl;