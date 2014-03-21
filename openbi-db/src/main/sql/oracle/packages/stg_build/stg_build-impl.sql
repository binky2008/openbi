CREATE OR REPLACE PACKAGE BODY stg_build
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-07-30 16:17:55 +0200 (Mo, 30 Jul 2012) $
    * $Revision: 3082 $
    * $Id: stg_build-impl.sql 3082 2012-07-30 14:17:55Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_build/stg_build-impl.sql $
    */
   PROCEDURE prc_build_all (
      p_vc_source_code    VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name    VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag   BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag   BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag   BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_stage_db_list      type.vc_max_plsql;
      l_vc_stage_owner_list   type.vc_max_plsql;
      l_vc_distr_code_list    type.vc_max_plsql;
      l_vc_col_def            type.vc_max_plsql;
      l_vc_col_all            type.vc_max_plsql;
      l_vc_col_pk             type.vc_max_plsql;
      l_vc_col_comm           type.vc_max_plsql;
      l_n_di_gui              NUMBER;
      l_n_step_no             NUMBER;
      l_n_result              NUMBER;
   BEGIN
   
      --trc.set_console_logging (FALSE);
       trc.log_info ('Set object names', 'Start');
      stg_meta.prc_set_object_properties;
       trc.log_info ('Set object names', 'Finish');
       trc.log_info ('Build objects', 'Start');

      -- Select all objects
      FOR r_obj IN (SELECT   s.stg_source_id
                           , s.stg_source_code
                           , s.stg_source_prefix
                           , d.stg_source_db_link
                           , d.stg_source_owner
                           , s.stg_owner
                           , s.stg_ts_stg1_data
                           , s.stg_ts_stg1_indx
                           , s.stg_ts_stg2_data
                           , s.stg_ts_stg2_indx
                           , s.stg_fb_archive
                           , o.stg_object_id
                           , o.stg_parallel_degree
                           , o.stg_source_nk_flag
                           , o.stg_object_name
                           , o.stg_object_comment
                           , o.stg_object_root
                           , o.stg_src_table_name
                           , o.stg_dupl_table_name
                           , o.stg_diff_table_name
                           , o.stg_diff_nk_name
                           , o.stg_stg1_table_name
                           , o.stg_stg2_table_name
                           , o.stg_stg2_nk_name
                           , o.stg_stg2_view_name
                           , o.stg_stg2_hist_name
                           , o.stg_package_name
                           , o.stg_filter_clause
                           , o.stg_partition_clause
                           , o.stg_fbda_flag
                           , o.stg_increment_buffer
                           , c.stg_increment_column
                           , c.stg_increment_coldef
                        FROM stg_source_t s
                           , (SELECT stg_source_id
                                   , stg_source_db_link
                                   , stg_source_owner
                                FROM (SELECT stg_source_id
                                           , stg_source_db_link
                                           , stg_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stg_source_id ORDER BY stg_source_db_id) AS source_db_order
                                        FROM stg_source_db_t)
                               WHERE source_db_order = 1) d
                           , stg_object_t o
                           , (SELECT stg_object_id
                                   , stg_column_name AS stg_increment_column
                                   , stg_column_def AS stg_increment_coldef
                                FROM (SELECT stg_object_id
                                           , stg_column_name
                                           , stg_column_def
                                           , ROW_NUMBER () OVER (PARTITION BY stg_object_id ORDER BY stg_column_pos) AS column_order
                                        FROM stg_column_t
                                       WHERE stg_column_incr_flag > 0
                                         AND (   stg_column_def LIKE 'DATE%'
                                              OR stg_column_def LIKE 'NUMBER%'))
                               WHERE column_order = 1) c
                       WHERE s.stg_source_id = d.stg_source_id
                         AND s.stg_source_id = o.stg_source_id
                         AND o.stg_object_id = c.stg_object_id(+)
                         AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stg_object_name, 'ALL')
                    ORDER BY stg_object_id)
      LOOP
          trc.log_info ('Object ' || r_obj.stg_object_name, 'Start');
         -- Reset list strings
         l_vc_stage_db_list                             := '';
         l_vc_stage_owner_list                          := '';
         l_vc_distr_code_list                           := '';
         l_vc_col_def                                   := '';
         l_vc_col_all                                   := '';
         l_vc_col_pk                                    := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stg_source_db_link
                           , stg_source_owner
                           , stg_distribution_code
                        FROM stg_source_db_t
                       WHERE stg_source_id = r_obj.stg_source_id)
         LOOP
            l_vc_stage_db_list       := l_vc_stage_db_list || r_db.stg_source_db_link || ',';
            l_vc_stage_owner_list    := l_vc_stage_owner_list || r_db.stg_source_owner || ',';
            l_vc_distr_code_list     := l_vc_distr_code_list || r_db.stg_distribution_code || ',';
         END LOOP;

         l_vc_stage_db_list                             := RTRIM (l_vc_stage_db_list, ',');
         l_vc_stage_owner_list                          := RTRIM (l_vc_stage_owner_list, ',');
         l_vc_distr_code_list                           := RTRIM (l_vc_distr_code_list, ',');

         -- Build list of columns
         FOR r_col IN (SELECT   NVL (stg_column_name_map, stg_column_name) AS stg_column_name
                              , stg_column_def
                              , stg_column_nk_pos
                           FROM stg_column_t
                          WHERE stg_object_id = r_obj.stg_object_id
                            AND stg_column_edwh_flag = 1
                       ORDER BY stg_column_pos)
         LOOP
            l_vc_col_def    := l_vc_col_def || CHR (10) || r_col.stg_column_name || ' ' || r_col.stg_column_def || ',';
            l_vc_col_all    := l_vc_col_all || CHR (10) || r_col.stg_column_name || ',';

            IF r_col.stg_column_nk_pos >= 0
            THEN
               l_vc_col_pk    := l_vc_col_pk || CHR (10) || r_col.stg_column_name || ',';
            END IF;
         END LOOP;

         l_vc_col_def                                   := RTRIM (l_vc_col_def, ',');
         l_vc_col_all                                   := RTRIM (l_vc_col_all, ',');
         l_vc_col_pk                                    := RTRIM (l_vc_col_pk, ',');
         -- Set main properties for the given object
         stg_ddl.g_n_object_id                := r_obj.stg_object_id;
         stg_ddl.g_n_parallel_degree          := r_obj.stg_parallel_degree;
         stg_ddl.g_n_source_nk_flag           := r_obj.stg_source_nk_flag;
         stg_ddl.g_n_fbda_flag                := r_obj.stg_fbda_flag;
         stg_ddl.g_vc_object_name             := r_obj.stg_object_name;
         stg_ddl.g_vc_table_comment           := r_obj.stg_object_comment;
         stg_ddl.g_vc_source_code             := r_obj.stg_source_code;
         stg_ddl.g_vc_prefix_src              := r_obj.stg_source_prefix;
         stg_ddl.g_vc_dblink                  := r_obj.stg_source_db_link;
         stg_ddl.g_vc_owner_src               := r_obj.stg_source_owner;
         stg_ddl.g_vc_owner_stg               := SYS_CONTEXT ('USERENV', 'CURRENT_USER');
         stg_ddl.g_vc_table_name_source       :=
                               CASE
                                  WHEN r_obj.stg_source_db_link IS NULL
                                  AND r_obj.stg_source_owner = r_obj.stg_owner
                                     THEN r_obj.stg_src_table_name
                                  ELSE r_obj.stg_object_name
                               END;
         --
         stg_ddl.g_vc_dedupl_rank_clause      :=
                                                       CASE
                                                          WHEN r_obj.stg_source_db_link IS NULL
                                                          AND r_obj.stg_source_owner = r_obj.stg_owner
                                                             THEN 'ORDER BY 1'
                                                          ELSE 'ORDER BY rowid DESC'
                                                       END;
         stg_ddl.g_vc_filter_clause           := r_obj.stg_filter_clause;
         stg_ddl.g_vc_partition_clause        := r_obj.stg_partition_clause;
         stg_ddl.g_vc_increment_column        := r_obj.stg_increment_column;
         stg_ddl.g_vc_increment_coldef        := r_obj.stg_increment_coldef;
         stg_ddl.g_n_increment_buffer         := r_obj.stg_increment_buffer;
         stg_ddl.g_vc_table_name_dupl         := r_obj.stg_dupl_table_name;
         stg_ddl.g_vc_table_name_diff         := r_obj.stg_diff_table_name;
         stg_ddl.g_vc_table_name_stage1       := r_obj.stg_stg1_table_name;
         stg_ddl.g_vc_table_name_stage2       := r_obj.stg_stg2_table_name;
         stg_ddl.g_vc_nk_name_diff            := r_obj.stg_diff_nk_name;
         stg_ddl.g_vc_nk_name_stage2          := r_obj.stg_stg2_nk_name;
         stg_ddl.g_vc_view_name_stage2        := r_obj.stg_stg2_view_name;
         stg_ddl.g_vc_view_name_history       := r_obj.stg_stg2_hist_name;
         stg_ddl.g_vc_package_main            := r_obj.stg_package_name;
         --
         stg_ddl.g_vc_col_def                 := l_vc_col_def;
         stg_ddl.g_vc_col_all                 := l_vc_col_all;
         stg_ddl.g_vc_col_pk_src              := l_vc_col_pk;
         --
         stg_ddl.g_vc_tablespace_stg1_data    := r_obj.stg_ts_stg1_data;
         stg_ddl.g_vc_tablespace_stg1_indx    := r_obj.stg_ts_stg1_indx;
         stg_ddl.g_vc_tablespace_stg2_data    := r_obj.stg_ts_stg2_data;
         stg_ddl.g_vc_tablespace_stg2_indx    := r_obj.stg_ts_stg2_indx;
         stg_ddl.g_vc_fb_archive              := r_obj.stg_fb_archive;
         --
         stg_ddl.g_l_dblink                   := type.fct_string_to_list (l_vc_stage_db_list, ',');
         stg_ddl.g_l_owner_src                := type.fct_string_to_list (l_vc_stage_owner_list, ',');
         stg_ddl.g_l_distr_code               := type.fct_string_to_list (l_vc_distr_code_list, ',');
         stg_ddl.g_vc_col_pk                  := CASE
                                                              WHEN l_vc_col_pk IS NOT NULL
                                                              AND stg_ddl.g_l_dblink.COUNT > 1
                                                                 THEN ' DI_REGION_ID,  '
                                                           END || l_vc_col_pk;
         -- Create target objects
         stg_ddl.prc_create_stage1_table (p_b_drop_st1_flag, p_b_raise_flag);
         stg_ddl.prc_create_stage2_table (p_b_drop_st2_flag, p_b_raise_flag);

         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            stg_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            stg_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;*/

         IF     stg_ddl.g_vc_fb_archive IS NOT NULL
            AND stg_ddl.g_n_fbda_flag = 1
         THEN
            stg_ddl.prc_create_stage2_hist (p_b_raise_flag);
         END IF;

         IF     l_vc_col_pk IS NOT NULL
            AND r_obj.stg_source_nk_flag = 0
         THEN
            stg_ddl.prc_create_duplicate_table (TRUE, p_b_raise_flag);
         END IF;

         stg_ddl.prc_create_diff_table (TRUE, p_b_raise_flag);
         stg_ddl.prc_create_package_main (FALSE, TRUE);
          trc.log_info ('Object ' || r_obj.stg_object_name, 'Finish');
      END LOOP;

   END prc_build_all;

   PROCEDURE prc_build_tc_only (
      p_vc_source_code    VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name    VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag   BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag   BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag   BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_stage_db_list      type.vc_max_plsql;
      l_vc_stage_owner_list   type.vc_max_plsql;
      l_vc_distr_code_list    type.vc_max_plsql;
      l_vc_col_def            type.vc_max_plsql;
      l_vc_col_all            type.vc_max_plsql;
      l_vc_col_pk             type.vc_max_plsql;
      l_vc_col_comm           type.vc_max_plsql;
      l_n_di_gui              NUMBER;
      l_n_step_no             NUMBER;
      l_n_result              NUMBER;
   BEGIN
      --trc.set_console_logging (FALSE);
       trc.log_info ('Set object names', 'Start');
      stg_meta.prc_set_object_properties;
       trc.log_info ('Set object names', 'Finish');
       trc.log_info ('Build objects', 'Start');

      -- Select all objects
      FOR r_obj IN (SELECT   s.stg_source_id
                           , s.stg_source_code
                           , s.stg_source_prefix
                           , d.stg_source_db_link
                           , d.stg_source_owner
                           , s.stg_owner
                           , s.stg_ts_stg1_data
                           , s.stg_ts_stg1_indx
                           , s.stg_ts_stg2_data
                           , s.stg_ts_stg2_indx
                           , s.stg_fb_archive
                           , o.stg_object_id
                           , o.stg_parallel_degree
                           , o.stg_source_nk_flag
                           , o.stg_object_name
                           , o.stg_object_comment
                           , o.stg_object_root
                           , o.stg_src_table_name
                           , o.stg_dupl_table_name
                           , o.stg_diff_table_name
                           , o.stg_diff_nk_name
                           , o.stg_stg1_table_name
                           , o.stg_stg2_table_name
                           , o.stg_stg2_nk_name
                           , o.stg_stg2_view_name
                           , o.stg_stg2_hist_name
                           , o.stg_package_name
                           , o.stg_filter_clause
                           , o.stg_partition_clause
                           , o.stg_fbda_flag
                        FROM stg_source_t s
                           , (SELECT stg_source_id
                                   , stg_source_db_link
                                   , stg_source_owner
                                FROM (SELECT stg_source_id
                                           , stg_source_db_link
                                           , stg_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stg_source_id ORDER BY stg_source_db_id) AS source_db_order
                                        FROM stg_source_db_t)
                               WHERE source_db_order = 1) d
                           , stg_object_t o
                       WHERE s.stg_source_id = d.stg_source_id(+)
                         AND s.stg_source_id = o.stg_source_id
                         AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stg_object_name, 'ALL')
                    ORDER BY stg_object_id)
      LOOP
          trc.log_info ('Object ' || r_obj.stg_object_name, 'Start');
         -- Reset list strings
         l_vc_stage_db_list                             := '';
         l_vc_stage_owner_list                          := '';
         l_vc_distr_code_list                           := '';
         l_vc_col_def                                   := '';
         l_vc_col_all                                   := '';
         l_vc_col_pk                                    := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stg_source_db_link
                           , stg_source_owner
                           , stg_distribution_code
                        FROM stg_source_db_t
                       WHERE stg_source_id = r_obj.stg_source_id)
         LOOP
            l_vc_stage_db_list       := l_vc_stage_db_list || r_db.stg_source_db_link || ',';
            l_vc_stage_owner_list    := l_vc_stage_owner_list || r_db.stg_source_owner || ',';
            l_vc_distr_code_list     := l_vc_distr_code_list || r_db.stg_distribution_code || ',';
         END LOOP;

         l_vc_stage_db_list                             := RTRIM (l_vc_stage_db_list, ',');
         l_vc_stage_owner_list                          := RTRIM (l_vc_stage_owner_list, ',');
         l_vc_distr_code_list                           := RTRIM (l_vc_distr_code_list, ',');

         -- Build list of columns
         FOR r_col IN (SELECT   NVL (stg_column_name_map, stg_column_name) AS stg_column_name
                              , stg_column_def
                              , stg_column_nk_pos
                           FROM stg_column_t
                          WHERE stg_object_id = r_obj.stg_object_id
                            AND stg_column_edwh_flag = 1
                       ORDER BY stg_column_pos)
         LOOP
            l_vc_col_def    := l_vc_col_def || CHR (10) || r_col.stg_column_name || ' ' || r_col.stg_column_def || ',';
            l_vc_col_all    := l_vc_col_all || CHR (10) || r_col.stg_column_name || ',';

            IF r_col.stg_column_nk_pos >= 0
            THEN
               l_vc_col_pk    := l_vc_col_pk || CHR (10) || r_col.stg_column_name || ',';
            END IF;
         END LOOP;

         l_vc_col_def                                   := RTRIM (l_vc_col_def, ',');
         l_vc_col_all                                   := RTRIM (l_vc_col_all, ',');
         l_vc_col_pk                                    := RTRIM (l_vc_col_pk, ',');
         -- Set main properties for the given object
         stg_ddl.g_n_object_id                := r_obj.stg_object_id;
         stg_ddl.g_n_parallel_degree          := r_obj.stg_parallel_degree;
         stg_ddl.g_n_source_nk_flag           := r_obj.stg_source_nk_flag;
         stg_ddl.g_vc_object_name             := r_obj.stg_object_name;
         stg_ddl.g_vc_table_comment           := r_obj.stg_object_comment;
         stg_ddl.g_vc_source_code             := r_obj.stg_source_code;
         stg_ddl.g_vc_prefix_src              := r_obj.stg_source_prefix;
         stg_ddl.g_vc_dblink                  := r_obj.stg_source_db_link;
         stg_ddl.g_vc_owner_src               := r_obj.stg_source_owner;
         stg_ddl.g_vc_owner_stg               := SYS_CONTEXT ('USERENV', 'CURRENT_USER');
         stg_ddl.g_vc_table_name_source       :=
                               CASE
                                  WHEN r_obj.stg_source_db_link IS NULL
                                  AND r_obj.stg_source_owner = r_obj.stg_owner
                                     THEN r_obj.stg_src_table_name
                                  ELSE r_obj.stg_object_name
                               END;
         --
         stg_ddl.g_vc_dedupl_rank_clause      :=
                                                       CASE
                                                          WHEN r_obj.stg_source_db_link IS NULL
                                                          AND r_obj.stg_source_owner = r_obj.stg_owner
                                                             THEN 'ORDER BY 1'
                                                          ELSE 'ORDER BY rowid DESC'
                                                       END;
         stg_ddl.g_vc_filter_clause           := r_obj.stg_filter_clause;
         stg_ddl.g_vc_partition_clause        := r_obj.stg_partition_clause;
         stg_ddl.g_vc_table_name_dupl         := r_obj.stg_dupl_table_name;
         stg_ddl.g_vc_table_name_diff         := r_obj.stg_diff_table_name;
         stg_ddl.g_vc_table_name_stage1       := r_obj.stg_stg1_table_name;
         stg_ddl.g_vc_table_name_stage2       := r_obj.stg_stg2_table_name;
         stg_ddl.g_vc_nk_name_diff            := r_obj.stg_diff_nk_name;
         stg_ddl.g_vc_nk_name_stage2          := r_obj.stg_stg2_nk_name;
         stg_ddl.g_vc_view_name_stage2        := r_obj.stg_stg2_view_name;
         stg_ddl.g_vc_view_name_history       := r_obj.stg_stg2_hist_name;
         stg_ddl.g_vc_package_main            := r_obj.stg_package_name;
         --
         stg_ddl.g_vc_col_def                 := l_vc_col_def;
         stg_ddl.g_vc_col_all                 := l_vc_col_all;
         stg_ddl.g_vc_col_pk_src              := l_vc_col_pk;
         --
         stg_ddl.g_vc_tablespace_stg1_data    := r_obj.stg_ts_stg1_data;
         stg_ddl.g_vc_tablespace_stg1_indx    := r_obj.stg_ts_stg1_indx;
         stg_ddl.g_vc_tablespace_stg2_data    := r_obj.stg_ts_stg2_data;
         stg_ddl.g_vc_tablespace_stg2_indx    := r_obj.stg_ts_stg2_indx;
         stg_ddl.g_vc_fb_archive              := r_obj.stg_fb_archive;
         stg_ddl.g_n_fbda_flag                := r_obj.stg_fbda_flag;
         --
         stg_ddl.g_l_dblink                   := type.fct_string_to_list (l_vc_stage_db_list, ',');
         stg_ddl.g_l_owner_src                := type.fct_string_to_list (l_vc_stage_owner_list, ',');
         stg_ddl.g_l_distr_code               := type.fct_string_to_list (l_vc_distr_code_list, ',');
         stg_ddl.g_vc_col_pk                  := CASE
                                                              WHEN l_vc_col_pk IS NOT NULL
                                                              AND stg_ddl.g_l_distr_code.COUNT > 1
                                                                 THEN ' DI_REGION_ID,  '
                                                           END || l_vc_col_pk;
         -- Create target objects
         stg_ddl.prc_create_stage1_table (p_b_drop_st1_flag, p_b_raise_flag);
         stg_ddl.prc_create_stage2_table (p_b_drop_st2_flag, p_b_raise_flag);

         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            stg_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            stg_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;*/

         IF     l_vc_col_pk IS NOT NULL
            AND r_obj.stg_source_nk_flag = 0
         THEN
            stg_ddl.prc_create_duplicate_table (TRUE, p_b_raise_flag);
         END IF;

         stg_ddl.prc_create_diff_table (TRUE, p_b_raise_flag);
         stg_ddl.prc_create_package_main (TRUE, TRUE);
          trc.log_info ('Object ' || r_obj.stg_object_name, 'Finish');
      END LOOP;

   END prc_build_tc_only;

   PROCEDURE prc_upgrade_stage2 (
      p_vc_source_code   VARCHAR2
    , p_vc_object_name   VARCHAR2
   )
   IS
      l_vc_stage_db_list     type.vc_max_plsql;
      l_vc_distr_code_list   type.vc_max_plsql;
      l_vc_col_def           type.vc_max_plsql;
      l_vc_col_pk            type.vc_max_plsql;
      l_vc_table_name_bkp    type.vc_obj_plsql;
      l_n_di_gui             NUMBER;
      l_n_step_no            NUMBER;
      l_n_result             NUMBER;
   BEGIN
      --trc.set_console_logging (FALSE);
       trc.log_info ('Set object names', 'Start');
      stg_meta.prc_set_object_properties;
       trc.log_info ('Set object names', 'Finish');
       trc.log_info ('Build objects', 'Start');

      -- Select all objects
      FOR r_obj IN (SELECT   s.stg_source_id
                           , s.stg_source_code
                           , s.stg_owner
                           , d.stg_source_db_link
                           , s.stg_ts_stg2_data
                           , s.stg_ts_stg2_indx
                           , o.stg_object_id
                           , stg_object_name
                           , o.stg_parallel_degree
                           , o.stg_stg2_table_name
                           , o.stg_stg2_view_name
                           , o.stg_stg2_nk_name
                           , o.stg_partition_clause
                        FROM stg_source_t s
                           , (SELECT stg_source_id
                                   , stg_source_db_link
                                   , stg_source_owner
                                FROM (SELECT stg_source_id
                                           , stg_source_db_link
                                           , stg_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stg_source_id ORDER BY stg_source_db_id) AS source_db_order
                                        FROM stg_source_db_t)
                               WHERE source_db_order = 1) d
                           , stg_object_t o
                       WHERE s.stg_source_id = d.stg_source_id
                         AND s.stg_source_id = o.stg_source_id
                         AND p_vc_source_code IN (s.stg_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stg_object_name, 'ALL')
                    ORDER BY stg_object_id)
      LOOP
          trc.log_info ('Object ' || r_obj.stg_object_name, 'Start');
         -- Reset list strings
         l_vc_stage_db_list                             := '';
         l_vc_distr_code_list                           := '';
         l_vc_col_def                                   := '';
         l_vc_col_pk                                    := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stg_source_db_link
                           , stg_source_owner
                           , stg_distribution_code
                        FROM stg_source_db_t
                       WHERE stg_source_id = r_obj.stg_source_id)
         LOOP
            l_vc_stage_db_list      := l_vc_stage_db_list || r_db.stg_source_db_link || ',';
            l_vc_distr_code_list    := l_vc_distr_code_list || r_db.stg_distribution_code || ',';
         END LOOP;

         l_vc_stage_db_list                             := RTRIM (l_vc_stage_db_list, ',');
         l_vc_distr_code_list                           := RTRIM (l_vc_distr_code_list, ',');

         -- Build list of columns
         FOR r_col IN (SELECT   NVL (stg_column_name_map, stg_column_name) AS stg_column_name
                              , stg_column_def
                              , stg_column_nk_pos
                           FROM stg_column_t
                          WHERE stg_object_id = r_obj.stg_object_id
                            AND stg_column_edwh_flag = 1
                       ORDER BY stg_column_pos)
         LOOP
            l_vc_col_def    := l_vc_col_def || CHR (10) || r_col.stg_column_name || ' ' || r_col.stg_column_def || ',';

            IF r_col.stg_column_nk_pos IS NOT NULL
            THEN
               l_vc_col_pk    := l_vc_col_pk || CHR (10) || r_col.stg_column_name || ',';
            END IF;
         END LOOP;

         l_vc_col_def                                   := RTRIM (l_vc_col_def, ',');
         l_vc_col_pk                                    := RTRIM (l_vc_col_pk, ',');
         -- Set main properties for the given object
         stg_ddl.g_n_parallel_degree          := r_obj.stg_parallel_degree;
         stg_ddl.g_vc_owner_stg               := SYS_CONTEXT ('USERENV', 'CURRENT_USER');
         --
         stg_ddl.g_vc_partition_clause        := r_obj.stg_partition_clause;
         stg_ddl.g_vc_table_name_stage2       := r_obj.stg_stg2_table_name;
         stg_ddl.g_vc_view_name_stage2        := r_obj.stg_stg2_view_name;
         stg_ddl.g_vc_nk_name_stage2          := r_obj.stg_stg2_nk_name;
         --
         stg_ddl.g_vc_col_def                 := l_vc_col_def;
         --
         stg_ddl.g_vc_tablespace_stg2_data    := r_obj.stg_ts_stg2_data;
         stg_ddl.g_vc_tablespace_stg2_indx    := r_obj.stg_ts_stg2_indx;
         --
         stg_ddl.g_l_dblink                   := type.fct_string_to_list (l_vc_stage_db_list, ',');
         stg_ddl.g_l_distr_code               := type.fct_string_to_list (l_vc_distr_code_list, ',');
         stg_ddl.g_vc_col_pk                  := CASE
                                                              WHEN stg_ddl.g_l_dblink.COUNT > 1
                                                                 THEN ' DI_REGION_ID,  '
                                                           END || l_vc_col_pk;

         -- Drop PK and indexes
         FOR r_cst IN (SELECT constraint_name
                         FROM all_constraints
                        WHERE owner = r_obj.stg_owner
                          AND table_name = r_obj.stg_stg2_table_name)
         LOOP
            EXECUTE IMMEDIATE 'ALTER TABLE ' || r_obj.stg_owner || '.' || r_obj.stg_stg2_table_name || ' DROP CONSTRAINT ' || r_cst.constraint_name;
         END LOOP;

         FOR r_idx IN (SELECT index_name
                         FROM all_indexes
                        WHERE owner = r_obj.stg_owner
                          AND table_name = r_obj.stg_stg2_table_name)
         LOOP
            EXECUTE IMMEDIATE 'DROP INDEX ' || r_obj.stg_owner || '.' || r_idx.index_name;
         END LOOP;

         l_vc_table_name_bkp                            := SUBSTR (r_obj.stg_stg2_table_name || '_BKP'
                                                                 , 1
                                                                 , 30
                                                                  );

         EXECUTE IMMEDIATE 'RENAME ' || r_obj.stg_stg2_table_name || ' TO ' || l_vc_table_name_bkp;

         -- Create target object
         stg_ddl.prc_create_stage2_table (FALSE, TRUE);
         -- Migrate data
         ddl.prc_migrate_table (r_obj.stg_stg2_table_name, l_vc_table_name_bkp);

         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            stg_ddl.prc_create_stage2_view (TRUE);
         ELSE
            stg_ddl.prc_create_stage2_synonym (TRUE);
         END IF;*/

          trc.log_info ('Object ' || r_obj.stg_object_name, 'Finish');
      END LOOP;

   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: stg_build-impl.sql 3082 2012-07-30 14:17:55Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_build/stg_build-impl.sql $';
END stg_build;
/

SHOW errors

BEGIN
   ddl.prc_create_synonym ('stg_build'
                                 , 'stg_build'
                                 , TRUE
                                  );
END;
/

SHOW errors