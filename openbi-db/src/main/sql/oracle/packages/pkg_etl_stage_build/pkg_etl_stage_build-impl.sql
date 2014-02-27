CREATE OR REPLACE PACKAGE BODY pkg_etl_stage_build
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-07-30 16:17:55 +0200 (Mo, 30 Jul 2012) $
    * $Revision: 3082 $
    * $Id: pkg_etl_stage_build-impl.sql 3082 2012-07-30 14:17:55Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_build/pkg_etl_stage_build-impl.sql $
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
      l_vc_stage_db_list      pkg_utl_type.vc_max_plsql;
      l_vc_stage_owner_list   pkg_utl_type.vc_max_plsql;
      l_vc_distr_code_list    pkg_utl_type.vc_max_plsql;
      l_vc_col_def            pkg_utl_type.vc_max_plsql;
      l_vc_col_all            pkg_utl_type.vc_max_plsql;
      l_vc_col_pk             pkg_utl_type.vc_max_plsql;
      l_vc_col_comm           pkg_utl_type.vc_max_plsql;
      l_n_di_gui              NUMBER;
      l_n_step_no             NUMBER;
      l_n_result              NUMBER;
   BEGIN
      pkg_utl_log.set_workflow_name ('STAGE_BUILD');
      l_n_result    := pkg_utl_job.initialize ('STAGE_BUILD'
                                             , 'STAGE_BUILD'
                                             , l_n_di_gui
                                             , l_n_step_no
                                              );
      l_n_result    := pkg_utl_job.set_step_no ('STAGE_BUILD'
                                              , 'STAGE_BUILD'
                                              , l_n_di_gui
                                              , 0
                                              , 'BEGIN'
                                               );
      pkg_utl_log.set_console_logging (FALSE);
      pkg_utl_log.LOG ('Set object names', 'Start');
      pkg_etl_stage_meta.prc_set_object_properties;
      pkg_utl_log.LOG ('Set object names', 'Finish');
      pkg_utl_log.LOG ('Build objects', 'Start');

      -- Select all objects
      FOR r_obj IN (SELECT   s.etl_stage_source_id
                           , s.etl_stage_source_code
                           , s.etl_stage_source_prefix
                           , d.etl_stage_source_db_link
                           , d.etl_stage_source_owner
                           , s.etl_stage_owner
                           , s.etl_stage_ts_stg1_data
                           , s.etl_stage_ts_stg1_indx
                           , s.etl_stage_ts_stg2_data
                           , s.etl_stage_ts_stg2_indx
                           , s.etl_stage_fb_archive
                           , o.etl_stage_object_id
                           , o.etl_stage_parallel_degree
                           , o.etl_stage_source_nk_flag
                           , o.etl_stage_object_name
                           , o.etl_stage_object_comment
                           , o.etl_stage_object_root
                           , o.etl_stage_src_table_name
                           , o.etl_stage_dupl_table_name
                           , o.etl_stage_diff_table_name
                           , o.etl_stage_diff_nk_name
                           , o.etl_stage_stg1_table_name
                           , o.etl_stage_stg2_table_name
                           , o.etl_stage_stg2_nk_name
                           , o.etl_stage_stg2_view_name
                           , o.etl_stage_stg2_hist_name
                           , o.etl_stage_package_name
                           , o.etl_stage_filter_clause
                           , o.etl_stage_partition_clause
                           , o.etl_stage_fbda_flag
                           , o.etl_stage_increment_buffer
                           , c.etl_stage_increment_column
                           , c.etl_stage_increment_coldef
                        FROM etl_stage_source_t s
                           , (SELECT etl_stage_source_id
                                   , etl_stage_source_db_link
                                   , etl_stage_source_owner
                                FROM (SELECT etl_stage_source_id
                                           , etl_stage_source_db_link
                                           , etl_stage_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY etl_stage_source_id ORDER BY etl_stage_source_db_id) AS source_db_order
                                        FROM etl_stage_source_db_t)
                               WHERE source_db_order = 1) d
                           , etl_stage_object_t o
                           , (SELECT etl_stage_object_id
                                   , etl_stage_column_name AS etl_stage_increment_column
                                   , etl_stage_column_def AS etl_stage_increment_coldef
                                FROM (SELECT etl_stage_object_id
                                           , etl_stage_column_name
                                           , etl_stage_column_def
                                           , ROW_NUMBER () OVER (PARTITION BY etl_stage_object_id ORDER BY etl_stage_column_pos) AS column_order
                                        FROM etl_stage_column_t
                                       WHERE etl_stage_column_incr_flag > 0
                                         AND (   etl_stage_column_def LIKE 'DATE%'
                                              OR etl_stage_column_def LIKE 'NUMBER%'))
                               WHERE column_order = 1) c
                       WHERE s.etl_stage_source_id = d.etl_stage_source_id
                         AND s.etl_stage_source_id = o.etl_stage_source_id
                         AND o.etl_stage_object_id = c.etl_stage_object_id(+)
                         AND p_vc_source_code IN (s.etl_stage_source_code, 'ALL')
                         AND p_vc_object_name IN (o.etl_stage_object_name, 'ALL')
                    ORDER BY etl_stage_object_id)
      LOOP
         pkg_utl_log.LOG ('Object ' || r_obj.etl_stage_object_name, 'Start');
         -- Reset list strings
         l_vc_stage_db_list                             := '';
         l_vc_stage_owner_list                          := '';
         l_vc_distr_code_list                           := '';
         l_vc_col_def                                   := '';
         l_vc_col_all                                   := '';
         l_vc_col_pk                                    := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT etl_stage_source_db_link
                           , etl_stage_source_owner
                           , etl_stage_distribution_code
                        FROM etl_stage_source_db_t
                       WHERE etl_stage_source_id = r_obj.etl_stage_source_id)
         LOOP
            l_vc_stage_db_list       := l_vc_stage_db_list || r_db.etl_stage_source_db_link || ',';
            l_vc_stage_owner_list    := l_vc_stage_owner_list || r_db.etl_stage_source_owner || ',';
            l_vc_distr_code_list     := l_vc_distr_code_list || r_db.etl_stage_distribution_code || ',';
         END LOOP;

         l_vc_stage_db_list                             := RTRIM (l_vc_stage_db_list, ',');
         l_vc_stage_owner_list                          := RTRIM (l_vc_stage_owner_list, ',');
         l_vc_distr_code_list                           := RTRIM (l_vc_distr_code_list, ',');

         -- Build list of columns
         FOR r_col IN (SELECT   NVL (etl_stage_column_name_map, etl_stage_column_name) AS etl_stage_column_name
                              , etl_stage_column_def
                              , etl_stage_column_nk_pos
                           FROM etl_stage_column_t
                          WHERE etl_stage_object_id = r_obj.etl_stage_object_id
                            AND etl_stage_column_edwh_flag = 1
                       ORDER BY etl_stage_column_pos)
         LOOP
            l_vc_col_def    := l_vc_col_def || CHR (10) || r_col.etl_stage_column_name || ' ' || r_col.etl_stage_column_def || ',';
            l_vc_col_all    := l_vc_col_all || CHR (10) || r_col.etl_stage_column_name || ',';

            IF r_col.etl_stage_column_nk_pos >= 0
            THEN
               l_vc_col_pk    := l_vc_col_pk || CHR (10) || r_col.etl_stage_column_name || ',';
            END IF;
         END LOOP;

         l_vc_col_def                                   := RTRIM (l_vc_col_def, ',');
         l_vc_col_all                                   := RTRIM (l_vc_col_all, ',');
         l_vc_col_pk                                    := RTRIM (l_vc_col_pk, ',');
         -- Set main properties for the given object
         pkg_etl_stage_ddl.g_n_object_id                := r_obj.etl_stage_object_id;
         pkg_etl_stage_ddl.g_n_parallel_degree          := r_obj.etl_stage_parallel_degree;
         pkg_etl_stage_ddl.g_n_source_nk_flag           := r_obj.etl_stage_source_nk_flag;
         pkg_etl_stage_ddl.g_n_fbda_flag                := r_obj.etl_stage_fbda_flag;
         pkg_etl_stage_ddl.g_vc_object_name             := r_obj.etl_stage_object_name;
         pkg_etl_stage_ddl.g_vc_table_comment           := r_obj.etl_stage_object_comment;
         pkg_etl_stage_ddl.g_vc_source_code             := r_obj.etl_stage_source_code;
         pkg_etl_stage_ddl.g_vc_prefix_src              := r_obj.etl_stage_source_prefix;
         pkg_etl_stage_ddl.g_vc_dblink                  := r_obj.etl_stage_source_db_link;
         pkg_etl_stage_ddl.g_vc_owner_src               := r_obj.etl_stage_source_owner;
         pkg_etl_stage_ddl.g_vc_owner_stg               := SYS_CONTEXT ('USERENV', 'CURRENT_USER');
         pkg_etl_stage_ddl.g_vc_table_name_source       :=
                               CASE
                                  WHEN r_obj.etl_stage_source_db_link IS NULL
                                  AND r_obj.etl_stage_source_owner = r_obj.etl_stage_owner
                                     THEN r_obj.etl_stage_src_table_name
                                  ELSE r_obj.etl_stage_object_name
                               END;
         --
         pkg_etl_stage_ddl.g_vc_dedupl_rank_clause      :=
                                                       CASE
                                                          WHEN r_obj.etl_stage_source_db_link IS NULL
                                                          AND r_obj.etl_stage_source_owner = r_obj.etl_stage_owner
                                                             THEN 'ORDER BY 1'
                                                          ELSE 'ORDER BY rowid DESC'
                                                       END;
         pkg_etl_stage_ddl.g_vc_filter_clause           := r_obj.etl_stage_filter_clause;
         pkg_etl_stage_ddl.g_vc_partition_clause        := r_obj.etl_stage_partition_clause;
         pkg_etl_stage_ddl.g_vc_increment_column        := r_obj.etl_stage_increment_column;
         pkg_etl_stage_ddl.g_vc_increment_coldef        := r_obj.etl_stage_increment_coldef;
         pkg_etl_stage_ddl.g_n_increment_buffer         := r_obj.etl_stage_increment_buffer;
         pkg_etl_stage_ddl.g_vc_table_name_dupl         := r_obj.etl_stage_dupl_table_name;
         pkg_etl_stage_ddl.g_vc_table_name_diff         := r_obj.etl_stage_diff_table_name;
         pkg_etl_stage_ddl.g_vc_table_name_stage1       := r_obj.etl_stage_stg1_table_name;
         pkg_etl_stage_ddl.g_vc_table_name_stage2       := r_obj.etl_stage_stg2_table_name;
         pkg_etl_stage_ddl.g_vc_nk_name_diff            := r_obj.etl_stage_diff_nk_name;
         pkg_etl_stage_ddl.g_vc_nk_name_stage2          := r_obj.etl_stage_stg2_nk_name;
         pkg_etl_stage_ddl.g_vc_view_name_stage2        := r_obj.etl_stage_stg2_view_name;
         pkg_etl_stage_ddl.g_vc_view_name_history       := r_obj.etl_stage_stg2_hist_name;
         pkg_etl_stage_ddl.g_vc_package_main            := r_obj.etl_stage_package_name;
         --
         pkg_etl_stage_ddl.g_vc_col_def                 := l_vc_col_def;
         pkg_etl_stage_ddl.g_vc_col_all                 := l_vc_col_all;
         pkg_etl_stage_ddl.g_vc_col_pk_src              := l_vc_col_pk;
         --
         pkg_etl_stage_ddl.g_vc_tablespace_stg1_data    := r_obj.etl_stage_ts_stg1_data;
         pkg_etl_stage_ddl.g_vc_tablespace_stg1_indx    := r_obj.etl_stage_ts_stg1_indx;
         pkg_etl_stage_ddl.g_vc_tablespace_stg2_data    := r_obj.etl_stage_ts_stg2_data;
         pkg_etl_stage_ddl.g_vc_tablespace_stg2_indx    := r_obj.etl_stage_ts_stg2_indx;
         pkg_etl_stage_ddl.g_vc_fb_archive              := r_obj.etl_stage_fb_archive;
         --
         pkg_etl_stage_ddl.g_l_dblink                   := pkg_utl_type.fct_string_to_list (l_vc_stage_db_list, ',');
         pkg_etl_stage_ddl.g_l_owner_src                := pkg_utl_type.fct_string_to_list (l_vc_stage_owner_list, ',');
         pkg_etl_stage_ddl.g_l_distr_code               := pkg_utl_type.fct_string_to_list (l_vc_distr_code_list, ',');
         pkg_etl_stage_ddl.g_vc_col_pk                  := CASE
                                                              WHEN l_vc_col_pk IS NOT NULL
                                                              AND pkg_etl_stage_ddl.g_l_dblink.COUNT > 1
                                                                 THEN ' DI_REGION_ID,  '
                                                           END || l_vc_col_pk;
         -- Create target objects
         pkg_etl_stage_ddl.prc_create_stage1_table (p_b_drop_st1_flag, p_b_raise_flag);
         pkg_etl_stage_ddl.prc_create_stage2_table (p_b_drop_st2_flag, p_b_raise_flag);

         -- Create view or synonym (depending on the environment)
         IF pkg_param.c_vc_db_name_actual IN (pkg_param.c_vc_db_name_dev, pkg_param.c_vc_db_name_tst)
         THEN
            pkg_etl_stage_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            pkg_etl_stage_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;

         IF     pkg_etl_stage_ddl.g_vc_fb_archive IS NOT NULL
            AND pkg_etl_stage_ddl.g_n_fbda_flag = 1
         THEN
            pkg_etl_stage_ddl.prc_create_stage2_hist (p_b_raise_flag);
         END IF;

         IF     l_vc_col_pk IS NOT NULL
            AND r_obj.etl_stage_source_nk_flag = 0
         THEN
            pkg_etl_stage_ddl.prc_create_duplicate_table (TRUE, p_b_raise_flag);
         END IF;

         pkg_etl_stage_ddl.prc_create_diff_table (TRUE, p_b_raise_flag);
         pkg_etl_stage_ddl.prc_create_package_main (FALSE, TRUE);
         pkg_utl_log.LOG ('Object ' || r_obj.etl_stage_object_name, 'Finish');
      END LOOP;

      l_n_result    := pkg_utl_job.set_step_no ('STAGE_BUILD'
                                              , 'STAGE_BUILD'
                                              , l_n_di_gui
                                              , 1
                                              , 'END'
                                               );
      l_n_result    := pkg_utl_job.finalize ('STAGE_BUILD'
                                           , 'STAGE_BUILD'
                                           , l_n_di_gui
                                            );
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
      l_vc_stage_db_list      pkg_utl_type.vc_max_plsql;
      l_vc_stage_owner_list   pkg_utl_type.vc_max_plsql;
      l_vc_distr_code_list    pkg_utl_type.vc_max_plsql;
      l_vc_col_def            pkg_utl_type.vc_max_plsql;
      l_vc_col_all            pkg_utl_type.vc_max_plsql;
      l_vc_col_pk             pkg_utl_type.vc_max_plsql;
      l_vc_col_comm           pkg_utl_type.vc_max_plsql;
      l_n_di_gui              NUMBER;
      l_n_step_no             NUMBER;
      l_n_result              NUMBER;
   BEGIN
      pkg_utl_log.set_workflow_name ('STAGE_BUILD');
      l_n_result    := pkg_utl_job.initialize ('STAGE_BUILD'
                                             , 'STAGE_BUILD'
                                             , l_n_di_gui
                                             , l_n_step_no
                                              );
      l_n_result    := pkg_utl_job.set_step_no ('STAGE_BUILD'
                                              , 'STAGE_BUILD'
                                              , l_n_di_gui
                                              , 0
                                              , 'BEGIN'
                                               );
      pkg_utl_log.set_console_logging (FALSE);
      pkg_utl_log.LOG ('Set object names', 'Start');
      pkg_etl_stage_meta.prc_set_object_properties;
      pkg_utl_log.LOG ('Set object names', 'Finish');
      pkg_utl_log.LOG ('Build objects', 'Start');

      -- Select all objects
      FOR r_obj IN (SELECT   s.etl_stage_source_id
                           , s.etl_stage_source_code
                           , s.etl_stage_source_prefix
                           , d.etl_stage_source_db_link
                           , d.etl_stage_source_owner
                           , s.etl_stage_owner
                           , s.etl_stage_ts_stg1_data
                           , s.etl_stage_ts_stg1_indx
                           , s.etl_stage_ts_stg2_data
                           , s.etl_stage_ts_stg2_indx
                           , s.etl_stage_fb_archive
                           , o.etl_stage_object_id
                           , o.etl_stage_parallel_degree
                           , o.etl_stage_source_nk_flag
                           , o.etl_stage_object_name
                           , o.etl_stage_object_comment
                           , o.etl_stage_object_root
                           , o.etl_stage_src_table_name
                           , o.etl_stage_dupl_table_name
                           , o.etl_stage_diff_table_name
                           , o.etl_stage_diff_nk_name
                           , o.etl_stage_stg1_table_name
                           , o.etl_stage_stg2_table_name
                           , o.etl_stage_stg2_nk_name
                           , o.etl_stage_stg2_view_name
                           , o.etl_stage_stg2_hist_name
                           , o.etl_stage_package_name
                           , o.etl_stage_filter_clause
                           , o.etl_stage_partition_clause
                           , o.etl_stage_fbda_flag
                        FROM etl_stage_source_t s
                           , (SELECT etl_stage_source_id
                                   , etl_stage_source_db_link
                                   , etl_stage_source_owner
                                FROM (SELECT etl_stage_source_id
                                           , etl_stage_source_db_link
                                           , etl_stage_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY etl_stage_source_id ORDER BY etl_stage_source_db_id) AS source_db_order
                                        FROM etl_stage_source_db_t)
                               WHERE source_db_order = 1) d
                           , etl_stage_object_t o
                       WHERE s.etl_stage_source_id = d.etl_stage_source_id(+)
                         AND s.etl_stage_source_id = o.etl_stage_source_id
                         AND p_vc_source_code IN (s.etl_stage_source_code, 'ALL')
                         AND p_vc_object_name IN (o.etl_stage_object_name, 'ALL')
                    ORDER BY etl_stage_object_id)
      LOOP
         pkg_utl_log.LOG ('Object ' || r_obj.etl_stage_object_name, 'Start');
         -- Reset list strings
         l_vc_stage_db_list                             := '';
         l_vc_stage_owner_list                          := '';
         l_vc_distr_code_list                           := '';
         l_vc_col_def                                   := '';
         l_vc_col_all                                   := '';
         l_vc_col_pk                                    := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT etl_stage_source_db_link
                           , etl_stage_source_owner
                           , etl_stage_distribution_code
                        FROM etl_stage_source_db_t
                       WHERE etl_stage_source_id = r_obj.etl_stage_source_id)
         LOOP
            l_vc_stage_db_list       := l_vc_stage_db_list || r_db.etl_stage_source_db_link || ',';
            l_vc_stage_owner_list    := l_vc_stage_owner_list || r_db.etl_stage_source_owner || ',';
            l_vc_distr_code_list     := l_vc_distr_code_list || r_db.etl_stage_distribution_code || ',';
         END LOOP;

         l_vc_stage_db_list                             := RTRIM (l_vc_stage_db_list, ',');
         l_vc_stage_owner_list                          := RTRIM (l_vc_stage_owner_list, ',');
         l_vc_distr_code_list                           := RTRIM (l_vc_distr_code_list, ',');

         -- Build list of columns
         FOR r_col IN (SELECT   NVL (etl_stage_column_name_map, etl_stage_column_name) AS etl_stage_column_name
                              , etl_stage_column_def
                              , etl_stage_column_nk_pos
                           FROM etl_stage_column_t
                          WHERE etl_stage_object_id = r_obj.etl_stage_object_id
                            AND etl_stage_column_edwh_flag = 1
                       ORDER BY etl_stage_column_pos)
         LOOP
            l_vc_col_def    := l_vc_col_def || CHR (10) || r_col.etl_stage_column_name || ' ' || r_col.etl_stage_column_def || ',';
            l_vc_col_all    := l_vc_col_all || CHR (10) || r_col.etl_stage_column_name || ',';

            IF r_col.etl_stage_column_nk_pos >= 0
            THEN
               l_vc_col_pk    := l_vc_col_pk || CHR (10) || r_col.etl_stage_column_name || ',';
            END IF;
         END LOOP;

         l_vc_col_def                                   := RTRIM (l_vc_col_def, ',');
         l_vc_col_all                                   := RTRIM (l_vc_col_all, ',');
         l_vc_col_pk                                    := RTRIM (l_vc_col_pk, ',');
         -- Set main properties for the given object
         pkg_etl_stage_ddl.g_n_object_id                := r_obj.etl_stage_object_id;
         pkg_etl_stage_ddl.g_n_parallel_degree          := r_obj.etl_stage_parallel_degree;
         pkg_etl_stage_ddl.g_n_source_nk_flag           := r_obj.etl_stage_source_nk_flag;
         pkg_etl_stage_ddl.g_vc_object_name             := r_obj.etl_stage_object_name;
         pkg_etl_stage_ddl.g_vc_table_comment           := r_obj.etl_stage_object_comment;
         pkg_etl_stage_ddl.g_vc_source_code             := r_obj.etl_stage_source_code;
         pkg_etl_stage_ddl.g_vc_prefix_src              := r_obj.etl_stage_source_prefix;
         pkg_etl_stage_ddl.g_vc_dblink                  := r_obj.etl_stage_source_db_link;
         pkg_etl_stage_ddl.g_vc_owner_src               := r_obj.etl_stage_source_owner;
         pkg_etl_stage_ddl.g_vc_owner_stg               := SYS_CONTEXT ('USERENV', 'CURRENT_USER');
         pkg_etl_stage_ddl.g_vc_table_name_source       :=
                               CASE
                                  WHEN r_obj.etl_stage_source_db_link IS NULL
                                  AND r_obj.etl_stage_source_owner = r_obj.etl_stage_owner
                                     THEN r_obj.etl_stage_src_table_name
                                  ELSE r_obj.etl_stage_object_name
                               END;
         --
         pkg_etl_stage_ddl.g_vc_dedupl_rank_clause      :=
                                                       CASE
                                                          WHEN r_obj.etl_stage_source_db_link IS NULL
                                                          AND r_obj.etl_stage_source_owner = r_obj.etl_stage_owner
                                                             THEN 'ORDER BY 1'
                                                          ELSE 'ORDER BY rowid DESC'
                                                       END;
         pkg_etl_stage_ddl.g_vc_filter_clause           := r_obj.etl_stage_filter_clause;
         pkg_etl_stage_ddl.g_vc_partition_clause        := r_obj.etl_stage_partition_clause;
         pkg_etl_stage_ddl.g_vc_table_name_dupl         := r_obj.etl_stage_dupl_table_name;
         pkg_etl_stage_ddl.g_vc_table_name_diff         := r_obj.etl_stage_diff_table_name;
         pkg_etl_stage_ddl.g_vc_table_name_stage1       := r_obj.etl_stage_stg1_table_name;
         pkg_etl_stage_ddl.g_vc_table_name_stage2       := r_obj.etl_stage_stg2_table_name;
         pkg_etl_stage_ddl.g_vc_nk_name_diff            := r_obj.etl_stage_diff_nk_name;
         pkg_etl_stage_ddl.g_vc_nk_name_stage2          := r_obj.etl_stage_stg2_nk_name;
         pkg_etl_stage_ddl.g_vc_view_name_stage2        := r_obj.etl_stage_stg2_view_name;
         pkg_etl_stage_ddl.g_vc_view_name_history       := r_obj.etl_stage_stg2_hist_name;
         pkg_etl_stage_ddl.g_vc_package_main            := r_obj.etl_stage_package_name;
         --
         pkg_etl_stage_ddl.g_vc_col_def                 := l_vc_col_def;
         pkg_etl_stage_ddl.g_vc_col_all                 := l_vc_col_all;
         pkg_etl_stage_ddl.g_vc_col_pk_src              := l_vc_col_pk;
         --
         pkg_etl_stage_ddl.g_vc_tablespace_stg1_data    := r_obj.etl_stage_ts_stg1_data;
         pkg_etl_stage_ddl.g_vc_tablespace_stg1_indx    := r_obj.etl_stage_ts_stg1_indx;
         pkg_etl_stage_ddl.g_vc_tablespace_stg2_data    := r_obj.etl_stage_ts_stg2_data;
         pkg_etl_stage_ddl.g_vc_tablespace_stg2_indx    := r_obj.etl_stage_ts_stg2_indx;
         pkg_etl_stage_ddl.g_vc_fb_archive              := r_obj.etl_stage_fb_archive;
         pkg_etl_stage_ddl.g_n_fbda_flag                := r_obj.etl_stage_fbda_flag;
         --
         pkg_etl_stage_ddl.g_l_dblink                   := pkg_utl_type.fct_string_to_list (l_vc_stage_db_list, ',');
         pkg_etl_stage_ddl.g_l_owner_src                := pkg_utl_type.fct_string_to_list (l_vc_stage_owner_list, ',');
         pkg_etl_stage_ddl.g_l_distr_code               := pkg_utl_type.fct_string_to_list (l_vc_distr_code_list, ',');
         pkg_etl_stage_ddl.g_vc_col_pk                  := CASE
                                                              WHEN l_vc_col_pk IS NOT NULL
                                                              AND pkg_etl_stage_ddl.g_l_distr_code.COUNT > 1
                                                                 THEN ' DI_REGION_ID,  '
                                                           END || l_vc_col_pk;
         -- Create target objects
         pkg_etl_stage_ddl.prc_create_stage1_table (p_b_drop_st1_flag, p_b_raise_flag);
         pkg_etl_stage_ddl.prc_create_stage2_table (p_b_drop_st2_flag, p_b_raise_flag);

         -- Create view or synonym (depending on the environment)
         IF pkg_param.c_vc_db_name_actual IN (pkg_param.c_vc_db_name_dev, pkg_param.c_vc_db_name_tst)
         THEN
            pkg_etl_stage_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            pkg_etl_stage_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;

         IF     l_vc_col_pk IS NOT NULL
            AND r_obj.etl_stage_source_nk_flag = 0
         THEN
            pkg_etl_stage_ddl.prc_create_duplicate_table (TRUE, p_b_raise_flag);
         END IF;

         pkg_etl_stage_ddl.prc_create_diff_table (TRUE, p_b_raise_flag);
         pkg_etl_stage_ddl.prc_create_package_main (TRUE, TRUE);
         pkg_utl_log.LOG ('Object ' || r_obj.etl_stage_object_name, 'Finish');
      END LOOP;

      l_n_result    := pkg_utl_job.set_step_no ('STAGE_BUILD'
                                              , 'STAGE_BUILD'
                                              , l_n_di_gui
                                              , 1
                                              , 'END'
                                               );
      l_n_result    := pkg_utl_job.finalize ('STAGE_BUILD'
                                           , 'STAGE_BUILD'
                                           , l_n_di_gui
                                            );
   END prc_build_tc_only;

   PROCEDURE prc_upgrade_stage2 (
      p_vc_source_code   VARCHAR2
    , p_vc_object_name   VARCHAR2
   )
   IS
      l_vc_stage_db_list     pkg_utl_type.vc_max_plsql;
      l_vc_distr_code_list   pkg_utl_type.vc_max_plsql;
      l_vc_col_def           pkg_utl_type.vc_max_plsql;
      l_vc_col_pk            pkg_utl_type.vc_max_plsql;
      l_vc_table_name_bkp    pkg_utl_type.vc_obj_plsql;
      l_n_di_gui             NUMBER;
      l_n_step_no            NUMBER;
      l_n_result             NUMBER;
   BEGIN
      pkg_utl_log.set_workflow_name ('STAGE_BUILD');
      l_n_result    := pkg_utl_job.initialize ('STAGE_BUILD'
                                             , 'STAGE_BUILD'
                                             , l_n_di_gui
                                             , l_n_step_no
                                              );
      l_n_result    := pkg_utl_job.set_step_no ('STAGE_BUILD'
                                              , 'STAGE_BUILD'
                                              , l_n_di_gui
                                              , 0
                                              , 'BEGIN'
                                               );
      pkg_utl_log.set_console_logging (FALSE);
      pkg_utl_log.LOG ('Set object names', 'Start');
      pkg_etl_stage_meta.prc_set_object_properties;
      pkg_utl_log.LOG ('Set object names', 'Finish');
      pkg_utl_log.LOG ('Build objects', 'Start');

      -- Select all objects
      FOR r_obj IN (SELECT   s.etl_stage_source_id
                           , s.etl_stage_source_code
                           , s.etl_stage_owner
                           , d.etl_stage_source_db_link
                           , s.etl_stage_ts_stg2_data
                           , s.etl_stage_ts_stg2_indx
                           , o.etl_stage_object_id
                           , etl_stage_object_name
                           , o.etl_stage_parallel_degree
                           , o.etl_stage_stg2_table_name
                           , o.etl_stage_stg2_view_name
                           , o.etl_stage_stg2_nk_name
                           , o.etl_stage_partition_clause
                        FROM etl_stage_source_t s
                           , (SELECT etl_stage_source_id
                                   , etl_stage_source_db_link
                                   , etl_stage_source_owner
                                FROM (SELECT etl_stage_source_id
                                           , etl_stage_source_db_link
                                           , etl_stage_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY etl_stage_source_id ORDER BY etl_stage_source_db_id) AS source_db_order
                                        FROM etl_stage_source_db_t)
                               WHERE source_db_order = 1) d
                           , etl_stage_object_t o
                       WHERE s.etl_stage_source_id = d.etl_stage_source_id
                         AND s.etl_stage_source_id = o.etl_stage_source_id
                         AND p_vc_source_code IN (s.etl_stage_source_code, 'ALL')
                         AND p_vc_object_name IN (o.etl_stage_object_name, 'ALL')
                    ORDER BY etl_stage_object_id)
      LOOP
         pkg_utl_log.LOG ('Object ' || r_obj.etl_stage_object_name, 'Start');
         -- Reset list strings
         l_vc_stage_db_list                             := '';
         l_vc_distr_code_list                           := '';
         l_vc_col_def                                   := '';
         l_vc_col_pk                                    := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT etl_stage_source_db_link
                           , etl_stage_source_owner
                           , etl_stage_distribution_code
                        FROM etl_stage_source_db_t
                       WHERE etl_stage_source_id = r_obj.etl_stage_source_id)
         LOOP
            l_vc_stage_db_list      := l_vc_stage_db_list || r_db.etl_stage_source_db_link || ',';
            l_vc_distr_code_list    := l_vc_distr_code_list || r_db.etl_stage_distribution_code || ',';
         END LOOP;

         l_vc_stage_db_list                             := RTRIM (l_vc_stage_db_list, ',');
         l_vc_distr_code_list                           := RTRIM (l_vc_distr_code_list, ',');

         -- Build list of columns
         FOR r_col IN (SELECT   NVL (etl_stage_column_name_map, etl_stage_column_name) AS etl_stage_column_name
                              , etl_stage_column_def
                              , etl_stage_column_nk_pos
                           FROM etl_stage_column_t
                          WHERE etl_stage_object_id = r_obj.etl_stage_object_id
                            AND etl_stage_column_edwh_flag = 1
                       ORDER BY etl_stage_column_pos)
         LOOP
            l_vc_col_def    := l_vc_col_def || CHR (10) || r_col.etl_stage_column_name || ' ' || r_col.etl_stage_column_def || ',';

            IF r_col.etl_stage_column_nk_pos IS NOT NULL
            THEN
               l_vc_col_pk    := l_vc_col_pk || CHR (10) || r_col.etl_stage_column_name || ',';
            END IF;
         END LOOP;

         l_vc_col_def                                   := RTRIM (l_vc_col_def, ',');
         l_vc_col_pk                                    := RTRIM (l_vc_col_pk, ',');
         -- Set main properties for the given object
         pkg_etl_stage_ddl.g_n_parallel_degree          := r_obj.etl_stage_parallel_degree;
         pkg_etl_stage_ddl.g_vc_owner_stg               := SYS_CONTEXT ('USERENV', 'CURRENT_USER');
         --
         pkg_etl_stage_ddl.g_vc_partition_clause        := r_obj.etl_stage_partition_clause;
         pkg_etl_stage_ddl.g_vc_table_name_stage2       := r_obj.etl_stage_stg2_table_name;
         pkg_etl_stage_ddl.g_vc_view_name_stage2        := r_obj.etl_stage_stg2_view_name;
         pkg_etl_stage_ddl.g_vc_nk_name_stage2          := r_obj.etl_stage_stg2_nk_name;
         --
         pkg_etl_stage_ddl.g_vc_col_def                 := l_vc_col_def;
         --
         pkg_etl_stage_ddl.g_vc_tablespace_stg2_data    := r_obj.etl_stage_ts_stg2_data;
         pkg_etl_stage_ddl.g_vc_tablespace_stg2_indx    := r_obj.etl_stage_ts_stg2_indx;
         --
         pkg_etl_stage_ddl.g_l_dblink                   := pkg_utl_type.fct_string_to_list (l_vc_stage_db_list, ',');
         pkg_etl_stage_ddl.g_l_distr_code               := pkg_utl_type.fct_string_to_list (l_vc_distr_code_list, ',');
         pkg_etl_stage_ddl.g_vc_col_pk                  := CASE
                                                              WHEN pkg_etl_stage_ddl.g_l_dblink.COUNT > 1
                                                                 THEN ' DI_REGION_ID,  '
                                                           END || l_vc_col_pk;

         -- Drop PK and indexes
         FOR r_cst IN (SELECT constraint_name
                         FROM all_constraints
                        WHERE owner = r_obj.etl_stage_owner
                          AND table_name = r_obj.etl_stage_stg2_table_name)
         LOOP
            EXECUTE IMMEDIATE 'ALTER TABLE ' || r_obj.etl_stage_owner || '.' || r_obj.etl_stage_stg2_table_name || ' DROP CONSTRAINT ' || r_cst.constraint_name;
         END LOOP;

         FOR r_idx IN (SELECT index_name
                         FROM all_indexes
                        WHERE owner = r_obj.etl_stage_owner
                          AND table_name = r_obj.etl_stage_stg2_table_name)
         LOOP
            EXECUTE IMMEDIATE 'DROP INDEX ' || r_obj.etl_stage_owner || '.' || r_idx.index_name;
         END LOOP;

         l_vc_table_name_bkp                            := SUBSTR (r_obj.etl_stage_stg2_table_name || '_BKP'
                                                                 , 1
                                                                 , 30
                                                                  );

         EXECUTE IMMEDIATE 'RENAME ' || r_obj.etl_stage_stg2_table_name || ' TO ' || l_vc_table_name_bkp;

         -- Create target object
         pkg_etl_stage_ddl.prc_create_stage2_table (FALSE, TRUE);
         -- Migrate data
         pkg_utl_ddl.prc_migrate_table (r_obj.etl_stage_stg2_table_name, l_vc_table_name_bkp);

         -- Create view or synonym (depending on the environment)
         IF pkg_param.c_vc_db_name_actual IN (pkg_param.c_vc_db_name_dev, pkg_param.c_vc_db_name_tst)
         THEN
            pkg_etl_stage_ddl.prc_create_stage2_view (TRUE);
         ELSE
            pkg_etl_stage_ddl.prc_create_stage2_synonym (TRUE);
         END IF;

         pkg_utl_log.LOG ('Object ' || r_obj.etl_stage_object_name, 'Finish');
      END LOOP;

      l_n_result    := pkg_utl_job.set_step_no ('STAGE_BUILD'
                                              , 'STAGE_BUILD'
                                              , l_n_di_gui
                                              , 1
                                              , 'END'
                                               );
      l_n_result    := pkg_utl_job.finalize ('STAGE_BUILD'
                                           , 'STAGE_BUILD'
                                           , l_n_di_gui
                                            );
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_etl_stage_build-impl.sql 3082 2012-07-30 14:17:55Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_build/pkg_etl_stage_build-impl.sql $';
END pkg_etl_stage_build;
/

SHOW errors

BEGIN
   pkg_utl_ddl.prc_create_synonym ('pkg_etl_stage_build'
                                 , 'pkg_etl_stage_build'
                                 , TRUE
                                  );
END;
/

SHOW errors