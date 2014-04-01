CREATE OR REPLACE PACKAGE BODY stag_build
AS
   /**
   * $Author: nmarangoni $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   PROCEDURE prc_build_all (
      p_vc_source_code     VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name     VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag    BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag    BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag       BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_stage_db_list      TYPE.vc_max_plsql;
      l_vc_stage_owner_list   TYPE.vc_max_plsql;
      l_vc_distr_code_list    TYPE.vc_max_plsql;
      l_vc_col_def            TYPE.vc_max_plsql;
      l_vc_col_all            TYPE.vc_max_plsql;
      l_vc_col_pk             TYPE.vc_max_plsql;
      l_vc_col_comm           TYPE.vc_max_plsql;
      l_n_di_gui              NUMBER;
      l_n_step_no             NUMBER;
      l_n_result              NUMBER;
   BEGIN
      --trac.set_console_logging (FALSE);
       trac.log_info (
         'Set object names'
       , 'Start'
      );
      stag_meta.prc_set_object_properties;
       trac.log_info (
         'Set object names'
       , 'Finish'
      );
       trac.log_info (
         'Build objects'
       , 'Start'
      );

      -- Select all objects
      FOR r_obj IN (  SELECT s.stag_source_id
                           , s.stag_source_code
                           , s.stag_source_prefix
                           , d.stag_source_db_link
                           , d.stag_source_owner
                           , s.stag_owner
                           , s.stag_ts_stg1_data
                           , s.stag_ts_stg1_indx
                           , s.stag_ts_stg2_data
                           , s.stag_ts_stg2_indx
                           , s.stag_fb_archive
                           , o.stag_object_id
                           , o.stag_parallel_degree
                           , o.stag_source_nk_flag
                           , o.stag_object_name
                           , o.stag_object_comment
                           , o.stag_object_root
                           , o.stag_src_table_name
                           , o.stag_dupl_table_name
                           , o.stag_diff_table_name
                           , o.stag_diff_nk_name
                           , o.stag_stg1_table_name
                           , o.stag_stg2_table_name
                           , o.stag_stg2_nk_name
                           , o.stag_stg2_view_name
                           , o.stag_stg2_hist_name
                           , o.stag_package_name
                           , o.stag_filter_clause
                           , o.stag_partition_clause
                           , o.stag_fbda_flag
                           , o.stag_increment_buffer
                           , c.stag_increment_column
                           , c.stag_increment_coldef
                        FROM stag_source_t s
                           , (SELECT stag_source_id
                                   , stag_source_db_link
                                   , stag_source_owner
                                FROM (SELECT stag_source_id
                                           , stag_source_db_link
                                           , stag_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stag_source_id ORDER BY stag_source_db_id) AS source_db_order
                                        FROM stag_source_db_t)
                               WHERE source_db_order = 1) d
                           , stag_object_t o
                           , (SELECT stag_object_id
                                   , stag_column_name AS stag_increment_column
                                   , stag_column_def AS stag_increment_coldef
                                FROM (SELECT stag_object_id
                                           , stag_column_name
                                           , stag_column_def
                                           , ROW_NUMBER () OVER (PARTITION BY stag_object_id ORDER BY stag_column_pos) AS column_order
                                        FROM stag_column_t
                                       WHERE stag_column_incr_flag > 0
                                         AND (stag_column_def LIKE 'DATE%'
                                           OR stag_column_def LIKE 'NUMBER%'))
                               WHERE column_order = 1) c
                       WHERE s.stag_source_id = d.stag_source_id
                         AND s.stag_source_id = o.stag_source_id
                         AND o.stag_object_id = c.stag_object_id(+)
                         AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stag_object_name, 'ALL')
                    ORDER BY stag_object_id) LOOP
          trac.log_info (
               'Object '
            || r_obj.stag_object_name
          , 'Start'
         );
         -- Reset list strings
         l_vc_stage_db_list := '';
         l_vc_stage_owner_list := '';
         l_vc_distr_code_list := '';
         l_vc_col_def := '';
         l_vc_col_all := '';
         l_vc_col_pk := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stag_source_db_link
                           , stag_source_owner
                           , stag_distribution_code
                        FROM stag_source_db_t
                       WHERE stag_source_id = r_obj.stag_source_id) LOOP
            l_vc_stage_db_list :=
                  l_vc_stage_db_list
               || r_db.stag_source_db_link
               || ',';
            l_vc_stage_owner_list :=
                  l_vc_stage_owner_list
               || r_db.stag_source_owner
               || ',';
            l_vc_distr_code_list :=
                  l_vc_distr_code_list
               || r_db.stag_distribution_code
               || ',';
         END LOOP;

         l_vc_stage_db_list :=
            RTRIM (
               l_vc_stage_db_list
             , ','
            );
         l_vc_stage_owner_list :=
            RTRIM (
               l_vc_stage_owner_list
             , ','
            );
         l_vc_distr_code_list :=
            RTRIM (
               l_vc_distr_code_list
             , ','
            );

         -- Build list of columns
         FOR r_col IN (  SELECT NVL (stag_column_name_map, stag_column_name) AS stag_column_name
                              , stag_column_def
                              , stag_column_nk_pos
                           FROM stag_column_t
                          WHERE stag_object_id = r_obj.stag_object_id
                            AND stag_column_edwh_flag = 1
                       ORDER BY stag_column_pos) LOOP
            l_vc_col_def :=
                  l_vc_col_def
               || CHR (10)
               || r_col.stag_column_name
               || ' '
               || r_col.stag_column_def
               || ',';
            l_vc_col_all :=
                  l_vc_col_all
               || CHR (10)
               || r_col.stag_column_name
               || ',';

            IF r_col.stag_column_nk_pos >= 0 THEN
               l_vc_col_pk :=
                     l_vc_col_pk
                  || CHR (10)
                  || r_col.stag_column_name
                  || ',';
            END IF;
         END LOOP;

         l_vc_col_def :=
            RTRIM (
               l_vc_col_def
             , ','
            );
         l_vc_col_all :=
            RTRIM (
               l_vc_col_all
             , ','
            );
         l_vc_col_pk :=
            RTRIM (
               l_vc_col_pk
             , ','
            );
         -- Set main properties for the given object
         stag_ddl.g_n_object_id := r_obj.stag_object_id;
         stag_ddl.g_n_parallel_degree := r_obj.stag_parallel_degree;
         stag_ddl.g_n_source_nk_flag := r_obj.stag_source_nk_flag;
         stag_ddl.g_n_fbda_flag := r_obj.stag_fbda_flag;
         stag_ddl.g_vc_object_name := r_obj.stag_object_name;
         stag_ddl.g_vc_table_comment := r_obj.stag_object_comment;
         stag_ddl.g_vc_source_code := r_obj.stag_source_code;
         stag_ddl.g_vc_prefix_src := r_obj.stag_source_prefix;
         stag_ddl.g_vc_dblink := r_obj.stag_source_db_link;
         stag_ddl.g_vc_owner_src := r_obj.stag_source_owner;
         stag_ddl.g_vc_owner_stg :=
            SYS_CONTEXT (
               'USERENV'
             , 'CURRENT_USER'
            );
         stag_ddl.g_vc_table_name_source :=
            CASE
               WHEN r_obj.stag_source_db_link IS NULL
                AND r_obj.stag_source_owner = r_obj.stag_owner THEN
                  r_obj.stag_src_table_name
               ELSE
                  r_obj.stag_object_name
            END;
         --
         stag_ddl.g_vc_dedupl_rank_clause :=
            CASE
               WHEN r_obj.stag_source_db_link IS NULL
                AND r_obj.stag_source_owner = r_obj.stag_owner THEN
                  'ORDER BY 1'
               ELSE
                  'ORDER BY rowid DESC'
            END;
         stag_ddl.g_vc_filter_clause := r_obj.stag_filter_clause;
         stag_ddl.g_vc_partition_clause := r_obj.stag_partition_clause;
         stag_ddl.g_vc_increment_column := r_obj.stag_increment_column;
         stag_ddl.g_vc_increment_coldef := r_obj.stag_increment_coldef;
         stag_ddl.g_n_increment_buffer := r_obj.stag_increment_buffer;
         stag_ddl.g_vc_table_name_dupl := r_obj.stag_dupl_table_name;
         stag_ddl.g_vc_table_name_diff := r_obj.stag_diff_table_name;
         stag_ddl.g_vc_table_name_stage1 := r_obj.stag_stg1_table_name;
         stag_ddl.g_vc_table_name_stage2 := r_obj.stag_stg2_table_name;
         stag_ddl.g_vc_nk_name_diff := r_obj.stag_diff_nk_name;
         stag_ddl.g_vc_nk_name_stage2 := r_obj.stag_stg2_nk_name;
         stag_ddl.g_vc_view_name_stage2 := r_obj.stag_stg2_view_name;
         stag_ddl.g_vc_view_name_history := r_obj.stag_stg2_hist_name;
         stag_ddl.g_vc_package_main := r_obj.stag_package_name;
         --
         stag_ddl.g_vc_col_def := l_vc_col_def;
         stag_ddl.g_vc_col_all := l_vc_col_all;
         stag_ddl.g_vc_col_pk_src := l_vc_col_pk;
         --
         stag_ddl.g_vc_tablespace_stg1_data := r_obj.stag_ts_stg1_data;
         stag_ddl.g_vc_tablespace_stg1_indx := r_obj.stag_ts_stg1_indx;
         stag_ddl.g_vc_tablespace_stg2_data := r_obj.stag_ts_stg2_data;
         stag_ddl.g_vc_tablespace_stg2_indx := r_obj.stag_ts_stg2_indx;
         stag_ddl.g_vc_fb_archive := r_obj.stag_fb_archive;
         --
         stag_ddl.g_l_dblink :=
            TYPE.fct_string_to_list (
               l_vc_stage_db_list
             , ','
            );
         stag_ddl.g_l_owner_src :=
            TYPE.fct_string_to_list (
               l_vc_stage_owner_list
             , ','
            );
         stag_ddl.g_l_distr_code :=
            TYPE.fct_string_to_list (
               l_vc_distr_code_list
             , ','
            );
         stag_ddl.g_vc_col_pk :=
               CASE
                  WHEN l_vc_col_pk IS NOT NULL
                   AND stag_ddl.g_l_dblink.COUNT > 1 THEN
                     ' DI_REGION_ID,  '
               END
            || l_vc_col_pk;
         -- Create target objects
         stag_ddl.prc_create_stage1_table (
            p_b_drop_st1_flag
          , p_b_raise_flag
         );
         stag_ddl.prc_create_stage2_table (
            p_b_drop_st2_flag
          , p_b_raise_flag
         );

         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            stag_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            stag_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;*/
         IF stag_ddl.g_vc_fb_archive IS NOT NULL
        AND stag_ddl.g_n_fbda_flag = 1 THEN
            stag_ddl.prc_create_stage2_hist (p_b_raise_flag);
         END IF;

         IF l_vc_col_pk IS NOT NULL
        AND r_obj.stag_source_nk_flag = 0 THEN
            stag_ddl.prc_create_duplicate_table (
               TRUE
             , p_b_raise_flag
            );
         END IF;

         stag_ddl.prc_create_diff_table (
            TRUE
          , p_b_raise_flag
         );
         stag_ddl.prc_create_package_main (
            FALSE
          , TRUE
         );
          trac.log_info (
               'Object '
            || r_obj.stag_object_name
          , 'Finish'
         );
      END LOOP;
   END prc_build_all;

   PROCEDURE prc_build_tc_only (
      p_vc_source_code     VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name     VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag    BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag    BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag       BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_stage_db_list      TYPE.vc_max_plsql;
      l_vc_stage_owner_list   TYPE.vc_max_plsql;
      l_vc_distr_code_list    TYPE.vc_max_plsql;
      l_vc_col_def            TYPE.vc_max_plsql;
      l_vc_col_all            TYPE.vc_max_plsql;
      l_vc_col_pk             TYPE.vc_max_plsql;
      l_vc_col_comm           TYPE.vc_max_plsql;
      l_n_di_gui              NUMBER;
      l_n_step_no             NUMBER;
      l_n_result              NUMBER;
   BEGIN
      --trac.set_console_logging (FALSE);
       trac.log_info (
         'Set object names'
       , 'Start'
      );
      stag_meta.prc_set_object_properties;
       trac.log_info (
         'Set object names'
       , 'Finish'
      );
       trac.log_info (
         'Build objects'
       , 'Start'
      );

      -- Select all objects
      FOR r_obj IN (  SELECT s.stag_source_id
                           , s.stag_source_code
                           , s.stag_source_prefix
                           , d.stag_source_db_link
                           , d.stag_source_owner
                           , s.stag_owner
                           , s.stag_ts_stg1_data
                           , s.stag_ts_stg1_indx
                           , s.stag_ts_stg2_data
                           , s.stag_ts_stg2_indx
                           , s.stag_fb_archive
                           , o.stag_object_id
                           , o.stag_parallel_degree
                           , o.stag_source_nk_flag
                           , o.stag_object_name
                           , o.stag_object_comment
                           , o.stag_object_root
                           , o.stag_src_table_name
                           , o.stag_dupl_table_name
                           , o.stag_diff_table_name
                           , o.stag_diff_nk_name
                           , o.stag_stg1_table_name
                           , o.stag_stg2_table_name
                           , o.stag_stg2_nk_name
                           , o.stag_stg2_view_name
                           , o.stag_stg2_hist_name
                           , o.stag_package_name
                           , o.stag_filter_clause
                           , o.stag_partition_clause
                           , o.stag_fbda_flag
                        FROM stag_source_t s
                           , (SELECT stag_source_id
                                   , stag_source_db_link
                                   , stag_source_owner
                                FROM (SELECT stag_source_id
                                           , stag_source_db_link
                                           , stag_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stag_source_id ORDER BY stag_source_db_id) AS source_db_order
                                        FROM stag_source_db_t)
                               WHERE source_db_order = 1) d
                           , stag_object_t o
                       WHERE s.stag_source_id = d.stag_source_id(+)
                         AND s.stag_source_id = o.stag_source_id
                         AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stag_object_name, 'ALL')
                    ORDER BY stag_object_id) LOOP
          trac.log_info (
               'Object '
            || r_obj.stag_object_name
          , 'Start'
         );
         -- Reset list strings
         l_vc_stage_db_list := '';
         l_vc_stage_owner_list := '';
         l_vc_distr_code_list := '';
         l_vc_col_def := '';
         l_vc_col_all := '';
         l_vc_col_pk := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stag_source_db_link
                           , stag_source_owner
                           , stag_distribution_code
                        FROM stag_source_db_t
                       WHERE stag_source_id = r_obj.stag_source_id) LOOP
            l_vc_stage_db_list :=
                  l_vc_stage_db_list
               || r_db.stag_source_db_link
               || ',';
            l_vc_stage_owner_list :=
                  l_vc_stage_owner_list
               || r_db.stag_source_owner
               || ',';
            l_vc_distr_code_list :=
                  l_vc_distr_code_list
               || r_db.stag_distribution_code
               || ',';
         END LOOP;

         l_vc_stage_db_list :=
            RTRIM (
               l_vc_stage_db_list
             , ','
            );
         l_vc_stage_owner_list :=
            RTRIM (
               l_vc_stage_owner_list
             , ','
            );
         l_vc_distr_code_list :=
            RTRIM (
               l_vc_distr_code_list
             , ','
            );

         -- Build list of columns
         FOR r_col IN (  SELECT NVL (stag_column_name_map, stag_column_name) AS stag_column_name
                              , stag_column_def
                              , stag_column_nk_pos
                           FROM stag_column_t
                          WHERE stag_object_id = r_obj.stag_object_id
                            AND stag_column_edwh_flag = 1
                       ORDER BY stag_column_pos) LOOP
            l_vc_col_def :=
                  l_vc_col_def
               || CHR (10)
               || r_col.stag_column_name
               || ' '
               || r_col.stag_column_def
               || ',';
            l_vc_col_all :=
                  l_vc_col_all
               || CHR (10)
               || r_col.stag_column_name
               || ',';

            IF r_col.stag_column_nk_pos >= 0 THEN
               l_vc_col_pk :=
                     l_vc_col_pk
                  || CHR (10)
                  || r_col.stag_column_name
                  || ',';
            END IF;
         END LOOP;

         l_vc_col_def :=
            RTRIM (
               l_vc_col_def
             , ','
            );
         l_vc_col_all :=
            RTRIM (
               l_vc_col_all
             , ','
            );
         l_vc_col_pk :=
            RTRIM (
               l_vc_col_pk
             , ','
            );
         -- Set main properties for the given object
         stag_ddl.g_n_object_id := r_obj.stag_object_id;
         stag_ddl.g_n_parallel_degree := r_obj.stag_parallel_degree;
         stag_ddl.g_n_source_nk_flag := r_obj.stag_source_nk_flag;
         stag_ddl.g_vc_object_name := r_obj.stag_object_name;
         stag_ddl.g_vc_table_comment := r_obj.stag_object_comment;
         stag_ddl.g_vc_source_code := r_obj.stag_source_code;
         stag_ddl.g_vc_prefix_src := r_obj.stag_source_prefix;
         stag_ddl.g_vc_dblink := r_obj.stag_source_db_link;
         stag_ddl.g_vc_owner_src := r_obj.stag_source_owner;
         stag_ddl.g_vc_owner_stg :=
            SYS_CONTEXT (
               'USERENV'
             , 'CURRENT_USER'
            );
         stag_ddl.g_vc_table_name_source :=
            CASE
               WHEN r_obj.stag_source_db_link IS NULL
                AND r_obj.stag_source_owner = r_obj.stag_owner THEN
                  r_obj.stag_src_table_name
               ELSE
                  r_obj.stag_object_name
            END;
         --
         stag_ddl.g_vc_dedupl_rank_clause :=
            CASE
               WHEN r_obj.stag_source_db_link IS NULL
                AND r_obj.stag_source_owner = r_obj.stag_owner THEN
                  'ORDER BY 1'
               ELSE
                  'ORDER BY rowid DESC'
            END;
         stag_ddl.g_vc_filter_clause := r_obj.stag_filter_clause;
         stag_ddl.g_vc_partition_clause := r_obj.stag_partition_clause;
         stag_ddl.g_vc_table_name_dupl := r_obj.stag_dupl_table_name;
         stag_ddl.g_vc_table_name_diff := r_obj.stag_diff_table_name;
         stag_ddl.g_vc_table_name_stage1 := r_obj.stag_stg1_table_name;
         stag_ddl.g_vc_table_name_stage2 := r_obj.stag_stg2_table_name;
         stag_ddl.g_vc_nk_name_diff := r_obj.stag_diff_nk_name;
         stag_ddl.g_vc_nk_name_stage2 := r_obj.stag_stg2_nk_name;
         stag_ddl.g_vc_view_name_stage2 := r_obj.stag_stg2_view_name;
         stag_ddl.g_vc_view_name_history := r_obj.stag_stg2_hist_name;
         stag_ddl.g_vc_package_main := r_obj.stag_package_name;
         --
         stag_ddl.g_vc_col_def := l_vc_col_def;
         stag_ddl.g_vc_col_all := l_vc_col_all;
         stag_ddl.g_vc_col_pk_src := l_vc_col_pk;
         --
         stag_ddl.g_vc_tablespace_stg1_data := r_obj.stag_ts_stg1_data;
         stag_ddl.g_vc_tablespace_stg1_indx := r_obj.stag_ts_stg1_indx;
         stag_ddl.g_vc_tablespace_stg2_data := r_obj.stag_ts_stg2_data;
         stag_ddl.g_vc_tablespace_stg2_indx := r_obj.stag_ts_stg2_indx;
         stag_ddl.g_vc_fb_archive := r_obj.stag_fb_archive;
         stag_ddl.g_n_fbda_flag := r_obj.stag_fbda_flag;
         --
         stag_ddl.g_l_dblink :=
            TYPE.fct_string_to_list (
               l_vc_stage_db_list
             , ','
            );
         stag_ddl.g_l_owner_src :=
            TYPE.fct_string_to_list (
               l_vc_stage_owner_list
             , ','
            );
         stag_ddl.g_l_distr_code :=
            TYPE.fct_string_to_list (
               l_vc_distr_code_list
             , ','
            );
         stag_ddl.g_vc_col_pk :=
               CASE
                  WHEN l_vc_col_pk IS NOT NULL
                   AND stag_ddl.g_l_distr_code.COUNT > 1 THEN
                     ' DI_REGION_ID,  '
               END
            || l_vc_col_pk;
         -- Create target objects
         stag_ddl.prc_create_stage1_table (
            p_b_drop_st1_flag
          , p_b_raise_flag
         );
         stag_ddl.prc_create_stage2_table (
            p_b_drop_st2_flag
          , p_b_raise_flag
         );

         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            stag_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            stag_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;*/
         IF l_vc_col_pk IS NOT NULL
        AND r_obj.stag_source_nk_flag = 0 THEN
            stag_ddl.prc_create_duplicate_table (
               TRUE
             , p_b_raise_flag
            );
         END IF;

         stag_ddl.prc_create_diff_table (
            TRUE
          , p_b_raise_flag
         );
         stag_ddl.prc_create_package_main (
            TRUE
          , TRUE
         );
          trac.log_info (
               'Object '
            || r_obj.stag_object_name
          , 'Finish'
         );
      END LOOP;
   END prc_build_tc_only;

   PROCEDURE prc_upgrade_stage2 (
      p_vc_source_code    VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
   IS
      l_vc_stage_db_list     TYPE.vc_max_plsql;
      l_vc_distr_code_list   TYPE.vc_max_plsql;
      l_vc_col_def           TYPE.vc_max_plsql;
      l_vc_col_pk            TYPE.vc_max_plsql;
      l_vc_table_name_bkp    TYPE.vc_obj_plsql;
      l_n_di_gui             NUMBER;
      l_n_step_no            NUMBER;
      l_n_result             NUMBER;
   BEGIN
      --trac.set_console_logging (FALSE);
       trac.log_info (
         'Set object names'
       , 'Start'
      );
      stag_meta.prc_set_object_properties;
       trac.log_info (
         'Set object names'
       , 'Finish'
      );
       trac.log_info (
         'Build objects'
       , 'Start'
      );

      -- Select all objects
      FOR r_obj IN (  SELECT s.stag_source_id
                           , s.stag_source_code
                           , s.stag_owner
                           , d.stag_source_db_link
                           , s.stag_ts_stg2_data
                           , s.stag_ts_stg2_indx
                           , o.stag_object_id
                           , stag_object_name
                           , o.stag_parallel_degree
                           , o.stag_stg2_table_name
                           , o.stag_stg2_view_name
                           , o.stag_stg2_nk_name
                           , o.stag_partition_clause
                        FROM stag_source_t s
                           , (SELECT stag_source_id
                                   , stag_source_db_link
                                   , stag_source_owner
                                FROM (SELECT stag_source_id
                                           , stag_source_db_link
                                           , stag_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stag_source_id ORDER BY stag_source_db_id) AS source_db_order
                                        FROM stag_source_db_t)
                               WHERE source_db_order = 1) d
                           , stag_object_t o
                       WHERE s.stag_source_id = d.stag_source_id
                         AND s.stag_source_id = o.stag_source_id
                         AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stag_object_name, 'ALL')
                    ORDER BY stag_object_id) LOOP
          trac.log_info (
               'Object '
            || r_obj.stag_object_name
          , 'Start'
         );
         -- Reset list strings
         l_vc_stage_db_list := '';
         l_vc_distr_code_list := '';
         l_vc_col_def := '';
         l_vc_col_pk := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stag_source_db_link
                           , stag_source_owner
                           , stag_distribution_code
                        FROM stag_source_db_t
                       WHERE stag_source_id = r_obj.stag_source_id) LOOP
            l_vc_stage_db_list :=
                  l_vc_stage_db_list
               || r_db.stag_source_db_link
               || ',';
            l_vc_distr_code_list :=
                  l_vc_distr_code_list
               || r_db.stag_distribution_code
               || ',';
         END LOOP;

         l_vc_stage_db_list :=
            RTRIM (
               l_vc_stage_db_list
             , ','
            );
         l_vc_distr_code_list :=
            RTRIM (
               l_vc_distr_code_list
             , ','
            );

         -- Build list of columns
         FOR r_col IN (  SELECT NVL (stag_column_name_map, stag_column_name) AS stag_column_name
                              , stag_column_def
                              , stag_column_nk_pos
                           FROM stag_column_t
                          WHERE stag_object_id = r_obj.stag_object_id
                            AND stag_column_edwh_flag = 1
                       ORDER BY stag_column_pos) LOOP
            l_vc_col_def :=
                  l_vc_col_def
               || CHR (10)
               || r_col.stag_column_name
               || ' '
               || r_col.stag_column_def
               || ',';

            IF r_col.stag_column_nk_pos IS NOT NULL THEN
               l_vc_col_pk :=
                     l_vc_col_pk
                  || CHR (10)
                  || r_col.stag_column_name
                  || ',';
            END IF;
         END LOOP;

         l_vc_col_def :=
            RTRIM (
               l_vc_col_def
             , ','
            );
         l_vc_col_pk :=
            RTRIM (
               l_vc_col_pk
             , ','
            );
         -- Set main properties for the given object
         stag_ddl.g_n_parallel_degree := r_obj.stag_parallel_degree;
         stag_ddl.g_vc_owner_stg :=
            SYS_CONTEXT (
               'USERENV'
             , 'CURRENT_USER'
            );
         --
         stag_ddl.g_vc_partition_clause := r_obj.stag_partition_clause;
         stag_ddl.g_vc_table_name_stage2 := r_obj.stag_stg2_table_name;
         stag_ddl.g_vc_view_name_stage2 := r_obj.stag_stg2_view_name;
         stag_ddl.g_vc_nk_name_stage2 := r_obj.stag_stg2_nk_name;
         --
         stag_ddl.g_vc_col_def := l_vc_col_def;
         --
         stag_ddl.g_vc_tablespace_stg2_data := r_obj.stag_ts_stg2_data;
         stag_ddl.g_vc_tablespace_stg2_indx := r_obj.stag_ts_stg2_indx;
         --
         stag_ddl.g_l_dblink :=
            TYPE.fct_string_to_list (
               l_vc_stage_db_list
             , ','
            );
         stag_ddl.g_l_distr_code :=
            TYPE.fct_string_to_list (
               l_vc_distr_code_list
             , ','
            );
         stag_ddl.g_vc_col_pk :=
               CASE
                  WHEN stag_ddl.g_l_dblink.COUNT > 1 THEN
                     ' DI_REGION_ID,  '
               END
            || l_vc_col_pk;

         -- Drop PK and indexes
         FOR r_cst IN (SELECT constraint_name
                         FROM all_constraints
                        WHERE owner = r_obj.stag_owner
                          AND table_name = r_obj.stag_stg2_table_name) LOOP
            EXECUTE IMMEDIATE
                  'ALTER TABLE '
               || r_obj.stag_owner
               || '.'
               || r_obj.stag_stg2_table_name
               || ' DROP CONSTRAINT '
               || r_cst.constraint_name;
         END LOOP;

         FOR r_idx IN (SELECT index_name
                         FROM all_indexes
                        WHERE owner = r_obj.stag_owner
                          AND table_name = r_obj.stag_stg2_table_name) LOOP
            EXECUTE IMMEDIATE
                  'DROP INDEX '
               || r_obj.stag_owner
               || '.'
               || r_idx.index_name;
         END LOOP;

         l_vc_table_name_bkp :=
            SUBSTR (
                  r_obj.stag_stg2_table_name
               || '_BKP'
             , 1
             , 30
            );

         EXECUTE IMMEDIATE
               'RENAME '
            || r_obj.stag_stg2_table_name
            || ' TO '
            || l_vc_table_name_bkp;

         -- Create target object
         stag_ddl.prc_create_stage2_table (
            FALSE
          , TRUE
         );
         -- Migrate data
         ddls.prc_migrate_table (
            r_obj.stag_stg2_table_name
          , l_vc_table_name_bkp
         );
         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            stag_ddl.prc_create_stage2_view (TRUE);
         ELSE
            stag_ddl.prc_create_stage2_synonym (TRUE);
         END IF;*/
          trac.log_info (
               'Object '
            || r_obj.stag_object_name
          , 'Finish'
         );
      END LOOP;
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: $';
   c_body_url := '$HeadURL: $';
END stag_build;
/

SHOW ERRORS