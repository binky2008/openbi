CREATE OR REPLACE PACKAGE BODY p#frm#stag_build
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: stag_build-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_build/stag_build-impl.sql $
   */
   /**
   * Object name type
   */
   SUBTYPE t_object_name IS VARCHAR2 (50);

   /**
   * String type
   */
   SUBTYPE t_string IS VARCHAR2 (32767);

   PROCEDURE prc_build_all (
      p_vc_source_code       VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name       VARCHAR2 DEFAULT 'ALL'
    , p_b_index_flag         BOOLEAN DEFAULT FALSE
    , p_b_drop_stage_flag    BOOLEAN DEFAULT TRUE
    , p_b_drop_hist_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag         BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name           t_object_name := 'prc_build_all';
      l_vc_stage_db_list      t_string;
      l_vc_stage_owner_list   t_string;
      l_vc_distr_code_list    t_string;
      l_vc_col_def            t_string;
      l_vc_col_all            t_string;
      l_vc_col_pk             t_string;
      l_vc_col_comm           t_string;
      --
      l_vc_col_hst            t_string;
      l_vc_col_upd            t_string;
   BEGIN
      --p#frm#trac.set_console_logging (FALSE);
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Start'
       , 'Build all db objects needed for a stage data flow'
      );
      p#frm#stag_meta.prc_set_object_properties;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Object properties'
       , 'Set names of db objects to be built'
      );
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Build objects'
       , 'Start building db objects'
      );

      -- Select all objects
      FOR r_obj IN (  SELECT s.stag_source_id
                           , s.stag_source_code
                           , s.stag_source_prefix
                           , d.stag_source_db_link
                           , d.stag_source_owner
                           , s.stag_owner
                           , s.stag_ts_stage_data
                           , s.stag_ts_stage_indx
                           , s.stag_ts_hist_data
                           , s.stag_ts_hist_indx
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
                           , o.stag_stage_table_name
                           , o.stag_hist_table_name
                           , o.stag_hist_nk_name
                           , o.stag_hist_view_name
                           , o.stag_hist_fbda_name
                           , o.stag_package_name
                           , o.stag_filter_clause
                           , o.stag_partition_clause
                           , o.stag_hist_flag
                           , o.stag_fbda_flag
                           , o.stag_increment_buffer
                           , c.stag_increment_column
                           , c.stag_increment_coldef
                        FROM p#frm#stag_source_t s
                           , (SELECT stag_source_id
                                   , stag_source_db_link
                                   , stag_source_owner
                                FROM (SELECT stag_source_id
                                           , stag_source_db_link
                                           , stag_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stag_source_id ORDER BY stag_source_db_id) AS source_db_order
                                        FROM p#frm#stag_source_db_t)
                               WHERE source_db_order = 1) d
                           , p#frm#stag_object_t o
                           , (SELECT stag_object_id
                                   , stag_column_name AS stag_increment_column
                                   , stag_column_def AS stag_increment_coldef
                                FROM (SELECT stag_object_id
                                           , stag_column_name
                                           , stag_column_def
                                           , ROW_NUMBER () OVER (PARTITION BY stag_object_id ORDER BY stag_column_pos) AS column_order
                                        FROM p#frm#stag_column_t
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
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          ,    'Object '
            || r_obj.stag_object_name
          , 'Start building objects'
         );
         -- Reset list strings
         l_vc_stage_db_list := '';
         l_vc_stage_owner_list := '';
         l_vc_distr_code_list := '';
         l_vc_col_def := '';
         l_vc_col_all := '';
         l_vc_col_pk := '';
         l_vc_col_hst := '';
         l_vc_col_upd := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stag_source_db_link
                           , stag_source_owner
                           , stag_distribution_code
                        FROM p#frm#stag_source_db_t
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
                              , stag_column_hist_flag
                           FROM p#frm#stag_column_t
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

            IF r_col.stag_column_hist_flag = 1
           AND r_obj.stag_hist_flag = 1 THEN
               l_vc_col_hst :=
                     l_vc_col_hst
                  || r_col.stag_column_name
                  || ',';
            ELSE
               l_vc_col_upd :=
                     l_vc_col_upd
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
         l_vc_col_hst :=
            RTRIM (
               l_vc_col_hst
             , ','
            );
         l_vc_col_upd :=
            RTRIM (
               l_vc_col_upd
             , ','
            );
         --
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , 'List of column definitions'
          , l_vc_col_def
         );
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , 'List of columns'
          , l_vc_col_all
         );
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , 'List of pk columns'
          , l_vc_col_pk
         );
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , 'List of columns to historicize'
          , l_vc_col_hst
         );
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , 'List of columns to update'
          , l_vc_col_upd
         );
         -- Set main properties for the given object
         p#frm#stag_ddl.g_n_object_id := r_obj.stag_object_id;
         p#frm#stag_ddl.g_n_parallel_degree := r_obj.stag_parallel_degree;
         p#frm#stag_ddl.g_n_source_nk_flag := r_obj.stag_source_nk_flag;
         p#frm#stag_ddl.g_n_fbda_flag := r_obj.stag_fbda_flag;
         p#frm#stag_ddl.g_vc_object_name := r_obj.stag_object_name;
         p#frm#stag_ddl.g_vc_table_comment := r_obj.stag_object_comment;
         p#frm#stag_ddl.g_vc_source_code := r_obj.stag_source_code;
         p#frm#stag_ddl.g_vc_prefix_src := r_obj.stag_source_prefix;
         p#frm#stag_ddl.g_vc_dblink := r_obj.stag_source_db_link;
         p#frm#stag_ddl.g_vc_owner_src := r_obj.stag_source_owner;
         p#frm#stag_ddl.g_vc_owner_stg := USER;
         p#frm#stag_ddl.g_vc_table_name_source :=
            CASE
               WHEN r_obj.stag_source_db_link IS NULL
                AND r_obj.stag_source_owner = r_obj.stag_owner THEN
                  r_obj.stag_src_table_name
               ELSE
                  r_obj.stag_object_name
            END;
         p#frm#stag_ddl.g_vc_source_identifier :=
            CASE
               WHEN r_obj.stag_source_db_link IS NULL
                AND r_obj.stag_source_owner = r_obj.stag_owner THEN
                  r_obj.stag_src_table_name
               ELSE
                     CASE
                        WHEN r_obj.stag_source_owner IS NOT NULL THEN
                              r_obj.stag_source_owner
                           || '.'
                     END
                  || r_obj.stag_object_name
                  || CASE
                        WHEN r_obj.stag_source_db_link IS NOT NULL THEN
                              '@'
                           || r_obj.stag_source_db_link
                     END
            END;
         --
         p#frm#stag_ddl.g_vc_dedupl_rank_clause :=
            CASE
               WHEN r_obj.stag_source_db_link IS NULL
                AND r_obj.stag_source_owner = r_obj.stag_owner THEN
                  'ORDER BY 1'
               ELSE
                  'ORDER BY rowid DESC'
            END;
         p#frm#stag_ddl.g_vc_filter_clause := r_obj.stag_filter_clause;
         p#frm#stag_ddl.g_vc_partition_expr := r_obj.stag_partition_clause;
         p#frm#stag_ddl.g_vc_increment_column := r_obj.stag_increment_column;
         p#frm#stag_ddl.g_vc_increment_coldef := r_obj.stag_increment_coldef;
         p#frm#stag_ddl.g_n_increment_buffer := r_obj.stag_increment_buffer;
         p#frm#stag_ddl.g_vc_table_name_dupl := r_obj.stag_dupl_table_name;
         p#frm#stag_ddl.g_vc_table_name_diff := r_obj.stag_diff_table_name;
         p#frm#stag_ddl.g_vc_table_name_stage := r_obj.stag_stage_table_name;
         p#frm#stag_ddl.g_vc_table_name_hist := r_obj.stag_hist_table_name;
         p#frm#stag_ddl.g_vc_nk_name_diff := r_obj.stag_diff_nk_name;
         p#frm#stag_ddl.g_vc_nk_name_hist := r_obj.stag_hist_nk_name;
         p#frm#stag_ddl.g_vc_view_name_hist := r_obj.stag_hist_view_name;
         p#frm#stag_ddl.g_vc_view_name_fbda := r_obj.stag_hist_fbda_name;
         p#frm#stag_ddl.g_vc_package_main := r_obj.stag_package_name;
         --
         p#frm#stag_ddl.g_vc_col_def := l_vc_col_def;
         p#frm#stag_ddl.g_vc_col_all := l_vc_col_all;
         p#frm#stag_ddl.g_vc_col_pk_src := l_vc_col_pk;
         --
         p#frm#stag_ddl.g_vc_col_hist := l_vc_col_hst;
         p#frm#stag_ddl.g_vc_col_update := l_vc_col_upd;
         --
         p#frm#stag_ddl.g_vc_tablespace_stage_data := r_obj.stag_ts_stage_data;
         p#frm#stag_ddl.g_vc_tablespace_stage_indx := r_obj.stag_ts_stage_indx;
         p#frm#stag_ddl.g_vc_tablespace_hist_data := r_obj.stag_ts_hist_data;
         p#frm#stag_ddl.g_vc_tablespace_hist_indx := r_obj.stag_ts_hist_indx;
         p#frm#stag_ddl.g_vc_fb_archive := r_obj.stag_fb_archive;
         --
         p#frm#stag_ddl.g_l_dblink :=
            p#frm#type.fct_string_to_list (
               l_vc_stage_db_list
             , ','
            );
         p#frm#stag_ddl.g_l_owner_src :=
            p#frm#type.fct_string_to_list (
               l_vc_stage_owner_list
             , ','
            );
         p#frm#stag_ddl.g_l_distr_code :=
            p#frm#type.fct_string_to_list (
               l_vc_distr_code_list
             , ','
            );
         p#frm#stag_ddl.g_vc_col_pk :=
               CASE
                  WHEN l_vc_col_pk IS NOT NULL
                   AND p#frm#stag_ddl.g_l_dblink.COUNT > 1 THEN
                        ' '
                     || p#frm#stag_param.c_vc_column_source_db
                     || ',  '
               END
            || l_vc_col_pk;
         -- Create target objects
         p#frm#stag_ddl.prc_create_stage_table (
            p_b_drop_stage_flag
          , p_b_raise_flag
         );
         p#frm#stag_ddl.prc_create_hist_table (
            p_b_drop_hist_flag
          , p_b_raise_flag
         );

         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            p#frm#stag_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            p#frm#stag_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;*/
         IF p#frm#stag_ddl.g_vc_fb_archive IS NOT NULL
        AND p#frm#stag_ddl.g_n_fbda_flag = 1 THEN
            p#frm#stag_ddl.prc_create_fbda_view (p_b_raise_flag);
         END IF;

         IF l_vc_col_pk IS NOT NULL
        AND r_obj.stag_source_nk_flag = 0 THEN
            p#frm#stag_ddl.prc_create_duplicate_table (
               TRUE
             , p_b_raise_flag
            );
         END IF;

         p#frm#stag_ddl.prc_create_diff_table (
            TRUE
          , p_b_raise_flag
         );
         p#frm#stag_ddl.prc_create_package_main (
            FALSE
          , TRUE
         );
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          ,    'Object '
            || r_obj.stag_object_name
          , 'Finish building db objects'
         );
      END LOOP;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Build objects'
       , 'Finished building db objects'
      );
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Finish'
       , 'Build complete'
      );
   END prc_build_all;

   PROCEDURE prc_build_hist (
      p_vc_source_code    VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name    VARCHAR2 DEFAULT 'ALL'
    , p_b_drop_flag       BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name           t_object_name := 'prc_build_hist';
      l_vc_stage_db_list      t_string;
      l_vc_stage_owner_list   t_string;
      l_vc_distr_code_list    t_string;
      --
      l_vc_col_def            t_string;
      l_vc_col_all            t_string;
      l_vc_col_pk             t_string;
      l_vc_col_comm           t_string;
      --
      l_vc_col_hst            t_string;
      l_vc_col_upd            t_string;
   BEGIN
      --p#frm#trac.set_console_logging (FALSE);
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Start'
       , 'Build db objects needed for the hist part of a stage data flow'
      );
      p#frm#stag_meta.prc_set_object_properties;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Object properties'
       , 'Set names of db objects to be built'
      );
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Build objects'
       , 'Start building db objects'
      );

      -- Select all objects
      FOR r_obj IN (  SELECT s.stag_source_id
                           , s.stag_source_code
                           , s.stag_source_prefix
                           , d.stag_source_db_link
                           , d.stag_source_owner
                           , s.stag_owner
                           , s.stag_ts_stage_data
                           , s.stag_ts_stage_indx
                           , s.stag_ts_hist_data
                           , s.stag_ts_hist_indx
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
                           , o.stag_stage_table_name
                           , o.stag_hist_table_name
                           , o.stag_hist_nk_name
                           , o.stag_hist_view_name
                           , o.stag_hist_fbda_name
                           , o.stag_package_name
                           , o.stag_filter_clause
                           , o.stag_partition_clause
                           , o.stag_fbda_flag
                        FROM p#frm#stag_source_t s
                           , (SELECT stag_source_id
                                   , stag_source_db_link
                                   , stag_source_owner
                                FROM (SELECT stag_source_id
                                           , stag_source_db_link
                                           , stag_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stag_source_id ORDER BY stag_source_db_id) AS source_db_order
                                        FROM p#frm#stag_source_db_t)
                               WHERE source_db_order = 1) d
                           , p#frm#stag_object_t o
                       WHERE s.stag_source_id = d.stag_source_id(+)
                         AND s.stag_source_id = o.stag_source_id
                         AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stag_object_name, 'ALL')
                    ORDER BY stag_object_id) LOOP
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          ,    'Object '
            || r_obj.stag_object_name
          , 'Start building objects'
         );
         -- Reset list strings
         l_vc_col_def := '';
         l_vc_col_all := '';
         l_vc_col_pk := '';

         -- Build list of columns
         FOR r_col IN (  SELECT NVL (stag_column_name_map, stag_column_name) AS stag_column_name
                              , stag_column_def
                              , stag_column_nk_pos
                              , stag_column_hist_flag
                           FROM p#frm#stag_column_t
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

            IF r_col.stag_column_hist_flag = 1 THEN
               l_vc_col_hst :=
                     l_vc_col_hst
                  || r_col.stag_column_name
                  || ',';
            ELSE
               l_vc_col_upd :=
                     l_vc_col_upd
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
         l_vc_col_hst :=
            RTRIM (
               l_vc_col_hst
             , ','
            );
         l_vc_col_upd :=
            RTRIM (
               l_vc_col_upd
             , ','
            );
         -- Set main properties for the given object
         p#frm#stag_ddl.g_n_object_id := r_obj.stag_object_id;
         p#frm#stag_ddl.g_n_parallel_degree := r_obj.stag_parallel_degree;
         p#frm#stag_ddl.g_n_source_nk_flag := r_obj.stag_source_nk_flag;
         p#frm#stag_ddl.g_vc_object_name := r_obj.stag_object_name;
         p#frm#stag_ddl.g_vc_table_comment := r_obj.stag_object_comment;
         p#frm#stag_ddl.g_vc_source_code := r_obj.stag_source_code;
         p#frm#stag_ddl.g_vc_prefix_src := r_obj.stag_source_prefix;
         p#frm#stag_ddl.g_vc_owner_stg := USER;
         p#frm#stag_ddl.g_vc_filter_clause := r_obj.stag_filter_clause;
         p#frm#stag_ddl.g_vc_partition_expr := r_obj.stag_partition_clause;
         p#frm#stag_ddl.g_vc_table_name_diff := r_obj.stag_diff_table_name;
         p#frm#stag_ddl.g_vc_table_name_stage := r_obj.stag_stage_table_name;
         p#frm#stag_ddl.g_vc_table_name_hist := r_obj.stag_hist_table_name;
         p#frm#stag_ddl.g_vc_nk_name_diff := r_obj.stag_diff_nk_name;
         p#frm#stag_ddl.g_vc_nk_name_hist := r_obj.stag_hist_nk_name;
         p#frm#stag_ddl.g_vc_view_name_hist := r_obj.stag_hist_view_name;
         p#frm#stag_ddl.g_vc_view_name_fbda := r_obj.stag_hist_fbda_name;
         p#frm#stag_ddl.g_vc_package_main := r_obj.stag_package_name;
         --
         p#frm#stag_ddl.g_vc_col_def := l_vc_col_def;
         p#frm#stag_ddl.g_vc_col_all := l_vc_col_all;
         p#frm#stag_ddl.g_vc_col_pk_src := l_vc_col_pk;
         --
         p#frm#stag_ddl.g_vc_tablespace_hist_data := r_obj.stag_ts_hist_data;
         p#frm#stag_ddl.g_vc_tablespace_hist_indx := r_obj.stag_ts_hist_indx;
         p#frm#stag_ddl.g_vc_fb_archive := r_obj.stag_fb_archive;
         p#frm#stag_ddl.g_n_fbda_flag := r_obj.stag_fbda_flag;
         --
         p#frm#stag_ddl.g_vc_col_pk :=
               CASE
                  WHEN l_vc_col_pk IS NOT NULL
                   AND p#frm#stag_ddl.g_l_distr_code.COUNT > 1 THEN
                        ' '
                     || p#frm#stag_param.c_vc_column_source_db
                     || ',  '
               END
            || l_vc_col_pk;
         -- Create target objects
         p#frm#stag_ddl.prc_create_hist_table (
            p_b_drop_flag
          , p_b_raise_flag
         );

         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            p#frm#stag_ddl.prc_create_stage2_view (p_b_raise_flag);
         ELSE
            p#frm#stag_ddl.prc_create_stage2_synonym (p_b_raise_flag);
         END IF;*/
         IF l_vc_col_pk IS NOT NULL
        AND r_obj.stag_source_nk_flag = 0 THEN
            p#frm#stag_ddl.prc_create_duplicate_table (
               TRUE
             , p_b_raise_flag
            );
         END IF;

         p#frm#stag_ddl.prc_create_diff_table (
            TRUE
          , p_b_raise_flag
         );
         p#frm#stag_ddl.prc_create_package_main (
            TRUE
          , TRUE
         );
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          ,    'Object '
            || r_obj.stag_object_name
          , 'Finish building db objects'
         );
      END LOOP;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Build objects'
       , 'Finished building db objects'
      );
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Finish'
       , 'Build complete'
      );
   END prc_build_hist;

   PROCEDURE prc_upgrade_hist (
      p_vc_source_code    VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
   IS
      l_vc_prc_name          t_object_name := 'prc_upgrade_hist';
      l_vc_stage_db_list     t_string;
      l_vc_distr_code_list   t_string;
      l_vc_col_def           t_string;
      l_vc_col_pk            t_string;
      l_vc_table_name_bkp    t_object_name;
      --
      l_vc_sql_statement     t_string;
      --
      l_n_cnt                NUMBER;
   BEGIN
      --p#frm#trac.set_console_logging (FALSE);
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Start'
       , 'Upgrade hist table with newly added columns'
      );
      p#frm#stag_meta.prc_set_object_properties;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Object properties'
       , 'Set names of db objects to be built'
      );
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Build objects'
       , 'Start building db objects'
      );
      p#frm#stag_ddl.g_vc_owner_stg := USER;

      --
      -- Select all objects
      FOR r_obj IN (  SELECT s.stag_source_id
                           , s.stag_source_code
                           --, s.stag_owner
                           , d.stag_source_db_link
                           , s.stag_ts_hist_data
                           , s.stag_ts_hist_indx
                           , o.stag_object_id
                           , stag_object_name
                           , o.stag_parallel_degree
                           , o.stag_hist_table_name
                           , o.stag_hist_view_name
                           , o.stag_hist_nk_name
                           , o.stag_partition_clause
                        FROM p#frm#stag_source_t s
                           , (SELECT stag_source_id
                                   , stag_source_db_link
                                   , stag_source_owner
                                FROM (SELECT stag_source_id
                                           , stag_source_db_link
                                           , stag_source_owner
                                           , ROW_NUMBER () OVER (PARTITION BY stag_source_id ORDER BY stag_source_db_id) AS source_db_order
                                        FROM p#frm#stag_source_db_t)
                               WHERE source_db_order = 1) d
                           , p#frm#stag_object_t o
                       WHERE s.stag_source_id = d.stag_source_id
                         AND s.stag_source_id = o.stag_source_id
                         AND p_vc_source_code IN (s.stag_source_code, 'ALL')
                         AND p_vc_object_name IN (o.stag_object_name, 'ALL')
                    ORDER BY stag_object_id) LOOP
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          ,    'Table:'
            || p#frm#stag_ddl.g_vc_owner_stg
            || '.'
            || r_obj.stag_hist_table_name
          , 'Start building objects'
         );
         -- Set name of the backup table
         l_vc_table_name_bkp :=
            SUBSTR (
                  r_obj.stag_hist_table_name
               || '_BKP'
             , 1
             , 30
            );

         SELECT COUNT (0)
           INTO l_n_cnt
           FROM all_tables
          WHERE owner = p#frm#stag_ddl.g_vc_owner_stg
            AND table_name = l_vc_table_name_bkp;

         IF l_n_cnt > 0 THEN
            raise_application_error (
               -20000
             , 'Backup table already present'
            );
         END IF;

         -- Reset list strings
         l_vc_stage_db_list := '';
         l_vc_distr_code_list := '';
         l_vc_col_def := '';
         l_vc_col_pk := '';

         -- Build list of values for objects with multiple sources
         FOR r_db IN (SELECT stag_source_db_link
                           , stag_source_owner
                           , stag_distribution_code
                        FROM p#frm#stag_source_db_t
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
                           FROM p#frm#stag_column_t
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
         p#frm#stag_ddl.g_n_parallel_degree := r_obj.stag_parallel_degree;
         p#frm#stag_ddl.g_vc_partition_expr := r_obj.stag_partition_clause;
         p#frm#stag_ddl.g_vc_table_name_hist := r_obj.stag_hist_table_name;
         p#frm#stag_ddl.g_vc_view_name_hist := r_obj.stag_hist_view_name;
         p#frm#stag_ddl.g_vc_nk_name_hist := r_obj.stag_hist_nk_name;
         --
         p#frm#stag_ddl.g_vc_col_def := l_vc_col_def;
         --
         p#frm#stag_ddl.g_vc_tablespace_hist_data := r_obj.stag_ts_hist_data;
         p#frm#stag_ddl.g_vc_tablespace_hist_indx := r_obj.stag_ts_hist_indx;
         --
         p#frm#stag_ddl.g_l_dblink :=
            p#frm#type.fct_string_to_list (
               l_vc_stage_db_list
             , ','
            );
         p#frm#stag_ddl.g_l_distr_code :=
            p#frm#type.fct_string_to_list (
               l_vc_distr_code_list
             , ','
            );
         p#frm#stag_ddl.g_vc_col_pk :=
               CASE
                  WHEN p#frm#stag_ddl.g_l_dblink.COUNT > 1 THEN
                        ' '
                     || p#frm#stag_param.c_vc_column_source_db
                     || ',  '
               END
            || l_vc_col_pk;

         -- Drop PK and indexes
         FOR r_cst IN (SELECT constraint_name
                         FROM all_constraints
                        WHERE owner = p#frm#stag_ddl.g_vc_owner_stg
                          AND table_name = r_obj.stag_hist_table_name) LOOP
            --
            l_vc_sql_statement :=
                  'ALTER TABLE '
               || p#frm#stag_ddl.g_vc_owner_stg
               || '.'
               || r_obj.stag_hist_table_name
               || ' DROP CONSTRAINT '
               || r_cst.constraint_name;
            --
            p#frm#trac.log_sub_debug (
               l_vc_prc_name
             , 'Drop constraint'
             , l_vc_sql_statement
            );

            --
            EXECUTE IMMEDIATE l_vc_sql_statement;
         --
         END LOOP;

         FOR r_idx IN (SELECT index_name
                         FROM all_indexes
                        WHERE owner = p#frm#stag_ddl.g_vc_owner_stg
                          AND table_name = r_obj.stag_hist_table_name) LOOP
            --
            l_vc_sql_statement :=
                  'DROP INDEX '
               || p#frm#stag_ddl.g_vc_owner_stg
               || '.'
               || r_idx.index_name;
            --
            p#frm#trac.log_sub_debug (
               l_vc_prc_name
             , 'Drop index'
             , l_vc_sql_statement
            );

            --
            EXECUTE IMMEDIATE l_vc_sql_statement;
         --
         END LOOP;

         EXECUTE IMMEDIATE
               'RENAME '
            || r_obj.stag_hist_table_name
            || ' TO '
            || l_vc_table_name_bkp;

         -- Create target object
         p#frm#stag_ddl.prc_create_hist_table (
            FALSE
          , TRUE
         );
         -- Migrate data
         p#frm#ddls.prc_migrate_table (
            r_obj.stag_hist_table_name
          , l_vc_table_name_bkp
         );
         -- Create view or synonym (depending on the environment)
         /*IF param.c_vc_db_name_actual IN (param.c_vc_db_name_dev, param.c_vc_db_name_tst)
         THEN
            p#frm#stag_ddl.prc_create_stage2_view (TRUE);
         ELSE
            p#frm#stag_ddl.prc_create_stage2_synonym (TRUE);
         END IF;*/
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          ,    'Object '
            || r_obj.stag_object_name
          , 'Finish building db objects'
         );
      END LOOP;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , 'Build objects'
       , 'Finished building db objects'
      );
      p#frm#trac.log_sub_info (
         l_vc_prc_name
       , 'Finish'
       , 'Build complete'
      );
   END;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: stag_build-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_build/stag_build-impl.sql $';
END p#frm#stag_build;