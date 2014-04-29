CREATE OR REPLACE PACKAGE BODY stag_ddl
AS
   /**
   * $Author: nmarangoni $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   /**
   * Templates for standard code tokens
   **/
   --
   c_token_utl_column_hist        VARCHAR2 (100) := '#validFromColumnName#, #validToColumnName#, #dmlOpColumnName#';
   c_token_utl_coldef_hist        VARCHAR2 (100) := '#validFromColumnName# DATE, #validToColumnName# DATE, #dmlOpColumnName# VARCHAR2(10)';
   c_token_utl_colval_hist        VARCHAR2 (100) := 'SYSDATE, TO_DATE(''99991231'',''yyyymmdd''), ''I''';
   c_token_utl_column_source_db   VARCHAR2 (100) := '#sourceDbColumnName#';
   c_token_utl_coldef_source_db   VARCHAR2 (100) := '#sourceDbColumnName# VARCHAR(100)';
   c_token_utl_column_partition   VARCHAR2 (100) := '#partitionColumnName#';
   c_token_utl_coldef_partition   VARCHAR2 (100) := '#partitionColumnName# NUMBER(1)';
   --
   c_token_diff_partition         CLOB
                                     :=    'PARTITION BY LIST ('
                                        || stag_param.c_vc_column_dml_op
                                        || ')
    (  
	  PARTITION PI VALUES (''I'') NOLOGGING NOCOMPRESS
    , PARTITION PU VALUES (''U'') NOLOGGING NOCOMPRESS
    , PARTITION PD VALUES (''D'') NOLOGGING NOCOMPRESS
    , PARTITION PR VALUES (''R'') NOLOGGING NOCOMPRESS
	)';
   c_token_diff_subpartition      CLOB
                                     :=    'PARTITION BY LIST (#partitionColumnName#)
    SUBPARTITION BY LIST ('
                                        || stag_param.c_vc_column_dml_op
                                        || ')
    SUBPARTITION TEMPLATE 
    (  
        SUBPARTITION PI VALUES (''I''),
        SUBPARTITION PU VALUES (''U''),
        SUBPARTITION PD VALUES (''D''),
        SUBPARTITION PR VALUES (''R'')
    )
    (
        PARTITION p0 VALUES (0) NOLOGGING NOCOMPRESS,
        PARTITION p1 VALUES (1) NOLOGGING NOCOMPRESS,
        PARTITION p2 VALUES (2) NOLOGGING NOCOMPRESS,
        PARTITION p3 VALUES (3) NOLOGGING NOCOMPRESS,
        PARTITION p4 VALUES (4) NOLOGGING NOCOMPRESS,
        PARTITION p5 VALUES (5) NOLOGGING NOCOMPRESS,
        PARTITION p6 VALUES (6) NOLOGGING NOCOMPRESS,
        PARTITION p7 VALUES (7) NOLOGGING NOCOMPRESS,
        PARTITION p8 VALUES (8) NOLOGGING NOCOMPRESS,
        PARTITION p9 VALUES (9) NOLOGGING NOCOMPRESS
    )';
   c_token_partition              CLOB := 'PARTITION BY LIST (#partitionColumnName#)
    (
        PARTITION p0 VALUES (0) NOLOGGING NOCOMPRESS,
        PARTITION p1 VALUES (1) NOLOGGING NOCOMPRESS,
        PARTITION p2 VALUES (2) NOLOGGING NOCOMPRESS,
        PARTITION p3 VALUES (3) NOLOGGING NOCOMPRESS,
        PARTITION p4 VALUES (4) NOLOGGING NOCOMPRESS,
        PARTITION p5 VALUES (5) NOLOGGING NOCOMPRESS,
        PARTITION p6 VALUES (6) NOLOGGING NOCOMPRESS,
        PARTITION p7 VALUES (7) NOLOGGING NOCOMPRESS,
        PARTITION p8 VALUES (8) NOLOGGING NOCOMPRESS,
        PARTITION p9 VALUES (9) NOLOGGING NOCOMPRESS
    )';
   -- Template to initialize run time statistics in a procedure
   -- Set the step number and the workflow
   c_token_prc_initialize         CLOB := '';
   -- Template to finalize run time statistics in a procedure
   -- Set the final step number and finalize job statistics
   c_token_prc_finalize           CLOB := '';
   -- Exception handler
   c_token_prc_exception          CLOB := 'stag_stat.prc_stat_end(l_n_stat_id, 0, 1);';
   -- Standard parameters for a generated procedure
   c_token_prc_param              CLOB := 'p_n_stream NUMBER DEFAULT NULL';
   -- Code body for the wrapper procedure
   c_token_prc_wrapper            CLOB := '
        trac.log_sub_debug (l_vc_prc_name, ''Staging Begin'', ''Start extracting from #tableName#'');

		#prcLoadStage#

        #prcLoadDiff#

		#prcLoadHist#

		#prcTruncStage#

		#prcTruncDiff#

        trac.log_sub_debug (l_vc_prc_name, ''Staging End'', ''Stage completed for #tableName#'');';
   -- Statistics token
   c_token_analyze                CLOB := '
        l_n_stat_id := stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', NULL, ''#statisticsType#'');
        
        DBMS_STATS.UNLOCK_TABLE_STATS (''#stgOwner#'', ''#tableName#'') ;
        DBMS_STATS.GATHER_TABLE_STATS (''#stgOwner#'', ''#tableName#'', NULL, 1);
        stag_stat.prc_size_store(''#sourceCode#'', ''#objectName#'', ''#tableName#'');

        stag_stat.prc_stat_end(l_n_stat_id, 0);

          trac.log_sub_debug (l_vc_prc_name, ''STAT END'', ''#tableName# : Statistics gathered'');
        ';
   -- Check token of the init procedure
   c_token_check_table_isempty    CLOB := '
		  trac.log_sub_debug (l_vc_prc_name, ''CHECK'', ''Check table #tableName# '');
        SELECT COUNT (*)
          INTO l_n_result
          FROM #tableName#
         WHERE rownum = 1;
         
        IF l_n_result = 0 THEN
              trac.log_sub_debug (l_vc_prc_name, ''CHECK'', ''Table #tableName# is empty'');
        ELSE
            trac.log_sub_error (l_vc_prc_name, ''CHECK'', ''Table #tableName# is not empty'');
            raise_application_error (-20000, ''Cannot init load non-empty table'');        
        END IF;';
   -- Insert token of the staging 1 procedure
   c_token_stage_get_incr_bound   CLOB := '
   
          trac.log_sub_debug (l_vc_prc_name, ''INCR BOUND'', ''#tableName# #partition# : get last #incrementColumn#'');
   
        SELECT MAX(#incrementColumn#)
          INTO l_t_increment_bound
          FROM #histTableName# #partition#;
          
          trac.log_sub_debug (l_vc_prc_name, ''INCR BOUND'', ''#tableName# #partition# : last #incrementColumn# = '' || l_t_increment_bound);
        
        ';
   -- Insert token of the staging procedure
   c_token_stage_insert           CLOB := '
        l_n_stat_id := stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''STIN'');

        #computeIncrementBound#
                   
        #insertStatement#

		l_n_result := SQL%ROWCOUNT;

		stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

        COMMIT;

        trac.log_sub_debug (l_vc_prc_name, ''INSERT END'', ''#targetIdentifier# #partition# : '' || l_n_result || '' rows inserted'', NULL, l_n_result);
		';
   -- Check token of the historicizing procedure
   c_token_check_nk_equal         CLOB := '
        l_b_ok := dict.fct_check_pk (
			NULL, ''#stgOwner#'', ''#stageTableName#'', ''#stgOwner#'', ''#histTableName#''
		);
		IF l_b_ok THEN
			  trac.log_sub_debug (l_vc_prc_name, ''CHECK NK'', ''#stageTableName# and #histTableName# have the same NK'');
		ELSE
			  trac.log_sub_warn (l_vc_prc_name, ''CHECK NK'', ''#stageTableName# and #histTableName# have not the same NK'');		
		END IF;
        
        SELECT COUNT(*) INTO l_n_result FROM #stageTableName#;
        
        IF l_n_result = 0 THEN
            trac.log_sub_error (l_vc_prc_name, ''CHECK'', ''Table #stageTableName# is empty'');
            raise_application_error (-20000, ''Stage1 table is empty.'');        
        END IF;
        
        EXECUTE IMMEDIATE ''ALTER SESSION ENABLE PARALLEL DML'';
		
		-- Truncate Diff table
		 trac.log_sub_debug (l_vc_prc_name, ''DIFF TRUNCATE'', ''Truncate #diffIdentifier#'');		
		EXECUTE IMMEDIATE ''TRUNCATE TABLE #diffIdentifier# DROP STORAGE'';
		 trac.log_sub_debug (l_vc_prc_name, ''DIFF TRUNCATE'', ''#diffIdentifier# truncated'');
		';
   -- Diff token of the historicizing procedure - with nk
   c_token_diff_insert            CLOB := '
		trac.log_sub_debug (l_vc_prc_name, ''DIFF BEGIN'', ''Insert into #diffIdentifier#'');

		l_n_stat_id := stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''DFIN'');
      
        #insertStatement#
        
		l_n_result := SQL%ROWCOUNT;

      COMMIT;

	  stag_stat.prc_stat_end(l_n_stat_id, l_n_result);
		
      trac.log_sub_debug (l_vc_prc_name, ''DIFF INSERTED'', ''#diffIdentifier# : '' || l_n_result || '' rows inserted'');
';
   -- Merge token of the historicizing procedure - 2 separate statement
   c_token_hist_reconcile         CLOB := '
        #enableParallelDML#
		
		-- Update Stage2 table
		
		trac.log_sub_debug (l_vc_prc_name, ''HIST UPDATE'', ''Update #histTableName#'');
		l_n_stat_id := stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''HSUP'');

        #updateStatement#

		l_n_result := SQL%ROWCOUNT;

		stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

        COMMIT;
		
        trac.log_sub_debug (l_vc_prc_name, ''HIST UPDATED'', ''#histTableName# : '' || l_n_result || '' rows updated'');
		
		-- Insert into Stage2 table
		
        trac.log_sub_debug (l_vc_prc_name, ''HIST INSERT'', ''#histTableName# : Insert'');

	    l_n_stat_id := stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''HSIN'');
        
        #insertStatement#

        l_n_result := SQL%ROWCOUNT;

        stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

	    COMMIT;

        trac.log_sub_debug (l_vc_prc_name, ''HIST END'', ''#histTableName# : '' || l_n_result || '' rows inserted'');';
   -- Buffers
   l_buffer_pkg_head              CLOB;
   l_buffer_pkg_body              CLOB;
   l_vc_col_src                   TYPE.vc_max_plsql;
   l_vc_col_dupl                  TYPE.vc_max_plsql;
   l_vc_col_pk_notnull            TYPE.vc_max_plsql;
   -- Anonymization
   l_vc_def_anonymized            TYPE.vc_max_plsql;
   l_vc_col_anonymized            TYPE.vc_max_plsql;
   l_vc_set_anonymized            TYPE.vc_max_plsql;
   l_vc_ins_anonymized            TYPE.vc_max_plsql;
   l_vc_fct_anonymized            TYPE.vc_max_plsql;
   l_vc_ini_anonymized            TYPE.vc_max_plsql;
   l_vc_viw_anonymized            TYPE.vc_max_plsql;

   FUNCTION fct_get_partition_db (p_vc_db_identifier VARCHAR2)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    CHR (10)
             || ' PARTITION '
             || stag_param.c_vc_prefix_partition
             || '_'
             || p_vc_db_identifier
             || ' VALUES ('''
             || p_vc_db_identifier
             || ''') NOLOGGING COMPRESS';
   END;

   FUNCTION fct_get_partition_expr
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    ' CASE WHEN TRIM( TRANSLATE ('
             || g_vc_partition_expr
             || ',''0123456789'',''          '')) IS NULL THEN TO_NUMBER('
             || g_vc_partition_expr
             || ') ELSE 0 END';
   END;

   PROCEDURE prc_set_utl_columns (p_vc_code_string IN OUT CLOB)
   IS
      l_vc_prc_name   TYPE.vc_max_plsql := 'prc_set_utl_columns';
   BEGIN
      ddls.prc_set_text_param (
         p_vc_code_string
       , 'validFromColumnName'
       , stag_param.c_vc_column_valid_from
      );
      ddls.prc_set_text_param (
         p_vc_code_string
       , 'validToColumnName'
       , stag_param.c_vc_column_valid_to
      );
      ddls.prc_set_text_param (
         p_vc_code_string
       , 'dmlOpColumnName'
       , stag_param.c_vc_column_dml_op
      );
      ddls.prc_set_text_param (
         p_vc_code_string
       , 'sourceDbColumnName'
       , stag_param.c_vc_column_source_db
      );
      ddls.prc_set_text_param (
         p_vc_code_string
       , 'partitionColumnName'
       , stag_param.c_vc_column_partition
      );
   END prc_set_utl_columns;

   -- Procedure to set column definition list in order to add anonymized columns to the stage2 table
   /*PROCEDURE prc_set_anonymized_coldefs
   IS
   BEGIN
      FOR r_col IN (SELECT   table_name
                           , src_column_name
                           , trg_column_name
                           , stag_column_def
                           , data_type
                           , data_length
                           , ora_function_name
                        FROM all_tab_columns exi
                           , (SELECT col.stag_object_id
                                   , col.stag_object_name
                                   , col.stag_stg2_table_name
                                   , col.stag_column_pos
                                   , col.stag_column_def
                                   , msk.src_column_name
                                   , msk.trg_column_name
                                   , msk.ora_function_name
                                FROM (SELECT o.stag_object_id
                                           , o.stag_object_name
                                           , o.stag_stg2_table_name
                                           , c.stag_column_pos
                                           , c.stag_column_name
                                           , c.stag_column_def
                                        FROM stag_object_t o
                                           , stag_column_t c
                                       WHERE o.stag_object_id = c.stag_object_id) col
                                   , (SELECT atab.table_name
                                           , acol.src_column_name
                                           , acol.trg_column_name
                                           , meth.ora_function_name
                                        FROM dmaskadmin.da_schema_v asch
                                           , dmaskadmin.da_table_v atab
                                           , dmaskadmin.da_column_v acol
                                           , dmaskadmin.da_business_attribute_v attr
                                           , dmaskadmin.da_method_v meth
                                       WHERE asch.schema_id = atab.schema_id
                                         AND atab.table_id = acol.table_id
                                         AND acol.business_attribute_id = attr.attribute_id
                                         AND attr.anonym_method_id = meth.method_id) msk
                               WHERE col.stag_stg2_table_name = msk.table_name
                                 AND col.stag_column_name = msk.src_column_name) met
                       WHERE met.stag_stg2_table_name = exi.table_name(+)
                         AND met.trg_column_name = exi.column_name(+)
                         AND exi.owner(+) = g_vc_owner_stg
                         AND exi.owner IS NULL
                    ORDER BY stag_column_pos)
      LOOP
         l_vc_def_anonymized    := l_vc_def_anonymized || ',' || r_col.trg_column_name || ' ' || r_col.stag_column_def;
         l_vc_ini_anonymized    :=
               l_vc_ini_anonymized
            || ','
            || r_col.trg_column_name
            || ' = '
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN 'SUBSTR('
               END
            || r_col.ora_function_name
            || '('
            || r_col.src_column_name
            || ')'
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN ',1,' || r_col.data_length || ')'
               END
            || CHR (10);
      END LOOP;

      NULL;
   END;

   -- Procedure to set column lists for stage2 update and insert statements
   PROCEDURE prc_set_anonymized_columns
   IS
   BEGIN
      FOR r_col IN (SELECT   msk.table_name
                           , msk.src_column_name
                           , msk.trg_column_name
                           , col.stag_column_def
                           , data_type
                           , data_length
                           , msk.ora_function_name
                        FROM all_tab_columns exi
                           , (SELECT o.stag_object_id
                                   , o.stag_object_name
                                   , o.stag_stg2_table_name
                                   , c.stag_column_pos
                                   , c.stag_column_name
                                   , c.stag_column_def
                                FROM stag_object_t o
                                   , stag_column_t c
                               WHERE o.stag_object_id = c.stag_object_id) col
                           , (SELECT atab.table_name
                                   , acol.src_column_name
                                   , acol.trg_column_name
                                   , meth.ora_function_name
                                FROM dmaskadmin.da_schema_v asch
                                   , dmaskadmin.da_table_v atab
                                   , dmaskadmin.da_column_v acol
                                   , dmaskadmin.da_business_attribute_v attr
                                   , dmaskadmin.da_method_v meth
                               WHERE asch.schema_id = atab.schema_id
                                 AND atab.table_id = acol.table_id
                                 AND acol.business_attribute_id = attr.attribute_id
                                 AND attr.anonym_method_id = meth.method_id) msk
                       WHERE col.stag_stg2_table_name = exi.table_name
                         AND col.stag_column_name = exi.column_name
                         AND col.stag_stg2_table_name = msk.table_name
                         AND col.stag_column_name = msk.src_column_name
                         AND col.stag_object_id = g_n_object_id
                         AND exi.owner = g_vc_owner_stg
                    ORDER BY stag_column_pos)
      LOOP
         l_vc_col_anonymized    := l_vc_col_anonymized || ',' || r_col.trg_column_name || CHR (10);
         l_vc_set_anonymized    :=
               l_vc_set_anonymized
            || ',trg.'
            || r_col.trg_column_name
            || ' = CASE WHEN dmaskadmin.pkg_da_anonymization_lib.is_ano_required('''
            || g_vc_owner_stg
            || ''','''
            || r_col.table_name
            || ''','''
            || r_col.src_column_name
            || ''','
            || r_col.src_column_name
            || ') = ''Y'' THEN'
            || CHR (10)
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN 'SUBSTR('
               END
            || 'dmaskadmin.'
            || r_col.ora_function_name
            || '(src.'
            || r_col.src_column_name
            || ')'
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN ',1,' || r_col.data_length || ')'
               END
            || 'ELSE src.'
            || r_col.src_column_name
            || CHR (10)
            || 'END';
         l_vc_ins_anonymized    :=
               l_vc_ins_anonymized
            || ',CASE WHEN dmaskadmin.pkg_da_anonymization_lib.is_ano_required('''
            || g_vc_owner_stg
            || ''','''
            || r_col.table_name
            || ''','''
            || r_col.src_column_name
            || ''','
            || r_col.src_column_name
            || ') = ''Y'' THEN'
            || CHR (10)
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN 'SUBSTR('
               END
            || 'dmaskadmin.'
            || r_col.ora_function_name
            || '(src.'
            || r_col.src_column_name
            || ')'
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN ',1,' || r_col.data_length || ')'
               END
            || CHR (10)
            || 'ELSE src.'
            || r_col.src_column_name
            || CHR (10)
            || 'END';
         l_vc_fct_anonymized    :=
               l_vc_fct_anonymized
            || ',CASE WHEN dmaskadmin.pkg_da_anonymization_lib.is_ano_required('''
            || g_vc_owner_stg
            || ''','''
            || r_col.table_name
            || ''','''
            || r_col.src_column_name
            || ''','
            || r_col.src_column_name
            || ') = ''Y'' THEN'
            || CHR (10)
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN 'SUBSTR('
               END
            || 'dmaskadmin.'
            || r_col.ora_function_name
            || '('
            || r_col.src_column_name
            || ')'
            || CASE
                  WHEN r_col.data_type LIKE '%CHAR%'
                     THEN ',1,' || r_col.data_length || ')'
               END
            || CHR (10)
            || 'ELSE '
            || r_col.src_column_name
            || CHR (10)
            || 'END';
      END LOOP;

      NULL;
   END;

   PROCEDURE prc_set_anonymized_viewcols
   IS
   BEGIN
      FOR r_col IN (SELECT   exi.table_name
                           , exi.column_name
                           , msk.trg_column_name
                        FROM all_tab_columns exi
                           , (SELECT atab.table_name
                                   , acol.src_column_name
                                   , acol.trg_column_name
                                   , meth.ora_function_name
                                FROM dmaskadmin.da_schema_v asch
                                   , dmaskadmin.da_table_v atab
                                   , dmaskadmin.da_column_v acol
                                   , dmaskadmin.da_business_attribute_v attr
                                   , dmaskadmin.da_method_v meth
                               WHERE asch.schema_id = atab.schema_id
                                 AND atab.table_id = acol.table_id
                                 AND acol.business_attribute_id = attr.attribute_id
                                 AND attr.anonym_method_id = meth.method_id) msk
                       WHERE exi.table_name = msk.table_name(+)
                         AND exi.column_name = msk.src_column_name(+)
                         AND exi.table_name = UPPER (g_vc_table_name_hist)
                         AND exi.owner = g_vc_owner_stg
                    ORDER BY exi.column_id)
      LOOP
         l_vc_viw_anonymized    :=
               l_vc_viw_anonymized
            || ','
            || CASE
                  WHEN pkg_param.c_vc_db_name_actual IN (pkg_param.c_vc_db_name_dev, pkg_param.c_vc_db_name_tst)
                  AND r_col.trg_column_name IS NOT NULL
                     THEN r_col.trg_column_name || ' AS ' || r_col.column_name
                  ELSE r_col.column_name
               END
            || CHR (10);
      END LOOP;
   END;*/
   PROCEDURE prc_store_ddl (
      p_vc_object_type    VARCHAR2
    , p_vc_object_name    VARCHAR2
    , p_vc_object_ddl     CLOB
   )
   IS
      l_vc_prc_name   TYPE.vc_max_plsql := 'prc_store_ddl';
   BEGIN
      MERGE INTO stag_ddl_t trg
           USING (SELECT UPPER (p_vc_object_type) AS object_type
                       , UPPER (p_vc_object_name) AS object_name
                       , p_vc_object_ddl AS object_ddl
                    FROM DUAL) src
              ON (UPPER (trg.stag_ddl_type) = UPPER (src.object_type)
              AND UPPER (trg.stag_ddl_name) = UPPER (src.object_name))
      WHEN MATCHED THEN
         UPDATE SET trg.stag_ddl_code = src.object_ddl
      WHEN NOT MATCHED THEN
         INSERT     (
                       trg.stag_ddl_type
                     , trg.stag_ddl_name
                     , trg.stag_ddl_code
                    )
             VALUES (
                       src.object_type
                     , src.object_name
                     , src.object_ddl
                    );

      COMMIT;
   END prc_store_ddl;

   PROCEDURE prc_create_stage_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name    TYPE.vc_max_plsql := 'prc_create_stage_table';
      l_vc_message     VARCHAR2 (32000)
                          :=    'Stage Table'
                             || g_vc_table_name_stage;
      l_sql_create     CLOB;
      l_list_utl_col   VARCHAR2 (32000);
   BEGIN
      trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Begin'
      );
      l_list_utl_col :=
         CASE
            WHEN g_l_distr_code.COUNT > 1 THEN
                  c_token_utl_coldef_source_db
               || ','
            WHEN g_vc_partition_expr IS NOT NULL THEN
                  c_token_utl_coldef_partition
               || ','
         END;
      -- Build create table statement
      l_sql_create := ddls.c_template_create_table;
      ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_stage
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       , g_vc_col_def
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'storageClause'
       ,    'NOLOGGING COMPRESS '
         || CASE
               WHEN g_vc_tablespace_stage_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stage_data
            END
      );

      -- Partitions
      IF g_l_distr_code.COUNT > 1 THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ' PARTITION BY LIST (#sourceDbColumnName#) (';

         FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
            IF i > 1 THEN
               l_sql_create :=
                     l_sql_create
                  || ',';
            END IF;

            l_sql_create :=
                  l_sql_create
               || fct_get_partition_db (g_l_distr_code (i));
         END LOOP;

         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ')';
      ELSIF g_vc_partition_expr IS NOT NULL THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || c_token_partition;
      END IF;

      prc_set_utl_columns (l_sql_create);
      prc_store_ddl (
         'TABLE'
       , g_vc_table_name_stage
       , l_sql_create
      );

      BEGIN
         trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Creating table'
         );
         ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_stage
          , l_sql_create
          , p_b_drop_flag
          , TRUE
         );
         trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Table created'
         );
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'Error creating'
            );
            RAISE;
      END;

      BEGIN
         trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Setting compression option...'
         );

         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_stage
            || ' COMPRESS FOR QUERY LOW';

         trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Compression option set'
         );
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'FOR QUERY LOW option not available'
            );
      END;

      -- Build constraint statement
      /*l_sql_create          := c_token_create_pk;
      ddls.prc_set_text_param (l_sql_create
                                    , 'tableName'
                                    , g_vc_table_name_stage
                                     );
      ddls.prc_set_text_param (l_sql_create
                                    , 'pkName'
                                    , g_vc_nk_name_stage
                                     );
      ddls.prc_set_text_param (l_sql_create
                                    , 'listColPk'
                                    , g_vc_col_pk
                                     );
      ddls.prc_set_text_param (l_sql_create
                                    , 'storageClause'
                                    , 'NOLOGGING ' || CASE
                                         WHEN g_l_distr_code.COUNT > 1
                                            THEN 'LOCAL'
                                      END || CASE
                                         WHEN g_vc_tablespace_stage_indx IS NOT NULL
                                            THEN ' TABLESPACE ' || g_vc_tablespace_stage_indx
                                      END
                                     );
      prc_set_utl_columns (l_sql_create);
      prc_store_ddl ('CONSTRAINT'
                   , g_vc_nk_name_stage
                   , l_sql_create
                    );

      BEGIN
           trac.log_info (l_vc_message, 'Creating NK...');
         ddls.prc_create_object ('CONSTRAINT'
                                      , g_vc_nk_name_stage
                                      , l_sql_create
                                      , p_b_drop_flag
                                      , TRUE
                                       );
           trac.log_info (l_vc_message, 'NK created');
      EXCEPTION
         WHEN OTHERS
         THEN
              trac.log_info (SQLERRM
                           , 'NK not created'
                           , param.gc_log_warn
                            );
            RAISE;
      END;*/
      IF g_n_parallel_degree > 1 THEN
         trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Setting parallel option...'
         );

         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_stage
            || ' PARALLEL '
            || g_n_parallel_degree;

         trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Parallel option set...'
         );
      END IF;

      -- Comments from source system
      trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Setting comments...'
      );

      EXECUTE IMMEDIATE
            'COMMENT ON TABLE '
         || g_vc_table_name_stage
         || ' IS '''
         || g_vc_table_comment
         || '''';

      FOR r_comm IN (SELECT c.stag_column_name
                          , c.stag_column_comment
                       FROM stag_object_t o
                          , stag_column_t c
                      WHERE o.stag_object_id = c.stag_object_id
                        AND o.stag_object_id = g_n_object_id) LOOP
         EXECUTE IMMEDIATE
               'COMMENT ON COLUMN '
            || g_vc_table_name_stage
            || '.'
            || r_comm.stag_column_name
            || ' IS '''
            || r_comm.stag_column_comment
            || '''';
      END LOOP;

      trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Comments set...'
      );
      trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         trac.log_sub_error (
            l_vc_prc_name
          , l_vc_message
          , 'Stage Table: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_create_stage_table;

   PROCEDURE prc_create_duplicate_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name    TYPE.vc_max_plsql := 'prc_create_duplicate_table';
      l_vc_message     VARCHAR2 (32000)
                          :=    'Table duplicates '
                             || g_vc_table_name_dupl;
      l_sql_create     CLOB;
      l_list_utl_col   VARCHAR2 (32000);
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Duplicates Table: Begin'
      );
      l_list_utl_col :=
         CASE
            WHEN g_l_distr_code.COUNT > 1 THEN
                  c_token_utl_coldef_source_db
               || ','
            WHEN g_vc_partition_expr IS NOT NULL THEN
                  c_token_utl_coldef_partition
               || ','
         END;
      -- Build create table statement
      l_sql_create := ddls.c_template_create_table;
      ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_dupl
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       , g_vc_col_def
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'storageClause'
       ,    'NOLOGGING'
         || CASE
               WHEN g_vc_tablespace_stage_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stage_data
            END
      );

      -- Stage1 partitions
      IF g_l_distr_code.COUNT > 1 THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ' PARTITION BY LIST (#sourceDbColumnName#) (';

         FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
            IF i > 1 THEN
               l_sql_create :=
                     l_sql_create
                  || ',';
            END IF;

            l_sql_create :=
                  l_sql_create
               || fct_get_partition_db (g_l_distr_code (i));
         END LOOP;

         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ')';
      ELSIF g_vc_partition_expr IS NOT NULL THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || c_token_partition;
      END IF;

      prc_set_utl_columns (l_sql_create);
      prc_store_ddl (
         'TABLE'
       , g_vc_table_name_dupl
       , l_sql_create
      );

      BEGIN
         ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_dupl
          , l_sql_create
          , p_b_drop_flag
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_error (
               'Duplicates Table: Warning'
             , SQLERRM
            );
            RAISE;
      END;

      IF g_n_parallel_degree > 1 THEN
         l_sql_create :=
               'ALTER TABLE '
            || g_vc_table_name_dupl
            || ' PARALLEL '
            || g_n_parallel_degree;
         ddls.prc_execute (l_sql_create);
      END IF;

      trac.log_info (
         l_vc_message
       , 'Duplicates Table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         trac.log_info (
            SQLERRM
          , 'Duplicates Table: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_create_duplicate_table;

   PROCEDURE prc_create_diff_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name            TYPE.vc_max_plsql := 'prc_create_diff_table';
      l_vc_message             VARCHAR2 (32000)
                                  :=    'Table difference '
                                     || g_vc_table_name_diff;
      l_sql_create             CLOB;
      l_sql_subpart_template   VARCHAR2 (32000);
      l_list_utl_col           VARCHAR2 (32000);
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Difference table: Begin'
      );
      l_list_utl_col :=
            c_token_utl_coldef_hist
         || ','
         || CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     c_token_utl_coldef_source_db
                  || ','
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     c_token_utl_coldef_partition
                  || ','
            END;
      l_sql_create := ddls.c_template_create_table;
      ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_diff
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       , g_vc_col_def
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'storageClause'
       ,    'NOLOGGING '
         || CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                  c_token_diff_subpartition
               ELSE
                  c_token_diff_partition
            END
         || CASE
               WHEN g_vc_tablespace_stage_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stage_data
            END
      );
      prc_set_utl_columns (l_sql_create);
      prc_store_ddl (
         'TABLE'
       , g_vc_table_name_diff
       , l_sql_create
      );

      BEGIN
         ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_diff
          , l_sql_create
          , p_b_drop_flag
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_error (
               'Difference Table: Warning'
             , SQLERRM
            );
            RAISE;
      END;

      l_sql_create := ddls.c_template_create_pk;
      ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_diff
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'pkName'
       , g_vc_nk_name_diff
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColPk'
       , g_vc_col_pk
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'storageClause'
       ,    'NOLOGGING'
         || CASE
               WHEN g_vc_tablespace_stage_indx IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stage_indx
            END
      );
      prc_store_ddl (
         'CONSTRAINT'
       , g_vc_nk_name_diff
       , l_sql_create
      );

      BEGIN
         ddls.prc_create_object (
            'CONSTRAINT'
          , g_vc_table_name_diff
          , l_sql_create
          , p_b_drop_flag
          , p_b_raise_flag
         );
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_error (
               'Difference table: Warning'
             , SQLERRM
            );
            RAISE;
      END;

      IF g_n_parallel_degree > 1 THEN
         l_sql_create :=
               'ALTER TABLE '
            || g_vc_table_name_diff
            || ' PARALLEL '
            || g_n_parallel_degree;
         ddls.prc_execute (l_sql_create);
      END IF;

      trac.log_info (
         l_vc_message
       , 'Difference table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         trac.log_info (
            SQLERRM
          , 'Difference table: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_create_diff_table;

   PROCEDURE prc_create_hist_table (
      p_b_drop_flag     BOOLEAN DEFAULT FALSE
    , p_b_raise_flag    BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name     TYPE.vc_max_plsql := 'prc_create_hist_table';
      l_vc_message      VARCHAR2 (32000)
                           :=    'History Table '
                              || g_vc_table_name_hist;
      l_sql_create      TYPE.vc_max_plsql;
      l_list_utl_col    TYPE.vc_max_plsql;
      l_l_utl_columns   DBMS_SQL.varchar2s;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Diff Table: Begin'
      );
      -- Set anonymizad column lists
      l_vc_def_anonymized := '';
      l_vc_ini_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_coldefs;
      -- Generate table ddl
      l_list_utl_col :=
            c_token_utl_coldef_hist
         || ','
         || CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     c_token_utl_coldef_source_db
                  || ','
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     c_token_utl_coldef_partition
                  || ','
            END;
      l_sql_create := ddls.c_template_create_table;
      ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_hist
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       ,    g_vc_col_def
         || l_vc_def_anonymized
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'storageClause'
       ,    'NOLOGGING COMPRESS '
         || CASE
               WHEN g_vc_tablespace_hist_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_hist_data
            END
      );

      IF g_vc_partition_expr IS NOT NULL THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || c_token_partition;
      END IF;

      prc_set_utl_columns (l_sql_create);
      -- Execute table ddl
      prc_store_ddl (
         'TABLE'
       , g_vc_table_name_hist
       , l_sql_create
      );

      BEGIN
         -- Try to create table
         ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_hist
          , l_sql_create
          , FALSE
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_error (
               'History Table Create: Warning'
             , SQLERRM
            );

            IF l_vc_def_anonymized IS NOT NULL THEN
               BEGIN
                  trac.log_info (
                     'Add new anonymized columns'
                   , 'History Table Add Anonymized'
                  );

                  -- Try to add newly anonymized columns
                  EXECUTE IMMEDIATE
                        'ALTER TABLE '
                     || g_vc_table_name_hist
                     || ' ADD ('
                     || LTRIM (
                           l_vc_def_anonymized
                         , ','
                        )
                     || ')';
               EXCEPTION
                  WHEN OTHERS THEN
                     trac.log_warn (
                        SQLERRM
                      , 'History Table Add Anonymized: Warning'
                     );

                     IF p_b_raise_flag THEN
                        RAISE;
                     END IF;
               END;
            END IF;

            IF l_vc_ini_anonymized IS NOT NULL THEN
               BEGIN
                  trac.log_info (
                     'Fill new anonymized columns'
                   , 'History Table Upd Anonymized'
                  );

                  -- Try to fill newly added anonymized columns
                  EXECUTE IMMEDIATE
                        'UPDATE '
                     || g_vc_table_name_hist
                     || ' SET '
                     || LTRIM (
                           l_vc_ini_anonymized
                         , ','
                        );

                  COMMIT;
               EXCEPTION
                  WHEN OTHERS THEN
                     trac.log_warn (
                        SQLERRM
                      , 'History Table Upd Anonymized: Warning'
                     );

                     IF p_b_raise_flag THEN
                        RAISE;
                     END IF;
               END;
            END IF;

            IF p_b_raise_flag THEN
               RAISE;
            END IF;
      END;

      IF g_n_parallel_degree > 1 THEN
         l_sql_create :=
               'ALTER TABLE '
            || g_vc_table_name_hist
            || ' PARALLEL '
            || g_n_parallel_degree;
         ddls.prc_execute (l_sql_create);
      END IF;

      IF g_vc_fb_archive IS NOT NULL
     AND g_n_fbda_flag = 1 THEN
         BEGIN
            EXECUTE IMMEDIATE
                  'ALTER TABLE '
               || g_vc_table_name_hist
               || ' FLASHBACK ARCHIVE '
               || g_vc_fb_archive;
         EXCEPTION
            WHEN OTHERS THEN
               trac.log_info (
                  SQLERRM
                , 'History Table: FLASHBACK'
               );
         END;
      END IF;

      BEGIN
         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_hist
            || ' COMPRESS FOR QUERY LOW';
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_info (
                  SQLERRM
               || ' - FOR QUERY LOW option not available'
             , 'History Table: COMPRESS'
            );
      END;

      -- Generate NK ddl
      l_sql_create := ddls.c_template_create_pk;
      ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_hist
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'pkName'
       , g_vc_nk_name_hist
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'listColPk'
       , g_vc_col_pk
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'storageClause'
       ,    'NOLOGGING '
         || CASE
               WHEN g_l_distr_code.COUNT > 1
                AND dict.fct_check_part (
                       NULL
                     , g_vc_owner_stg
                     , g_vc_table_name_hist
                    ) THEN
                  'LOCAL'
            END
         || CASE
               WHEN g_vc_tablespace_hist_indx IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_hist_indx
            END
      );
      -- Execute NK ddl
      prc_store_ddl (
         'CONSTRAINT'
       , g_vc_nk_name_hist
       , l_sql_create
      );

      BEGIN
         ddls.prc_create_object (
            'CONSTRAINT'
          , g_vc_nk_name_hist
          , l_sql_create
          , FALSE
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            trac.log_warn (
               SQLERRM
             , 'Stage 2 Natural Key: Warning'
            );

            IF p_b_raise_flag THEN
               RAISE;
            END IF;
      END;

      -- Create not null constraints
      l_l_utl_columns :=
         TYPE.fct_string_to_list (
            c_token_utl_column_hist
          , ','
         );

      FOR i IN l_l_utl_columns.FIRST .. l_l_utl_columns.LAST LOOP
         l_sql_create := ddls.c_template_create_notnull;
         ddls.prc_set_text_param (
            l_sql_create
          , 'tableName'
          , g_vc_table_name_hist
         );
         ddls.prc_set_text_param (
            l_sql_create
          , 'columnName'
          , l_l_utl_columns (i)
         );
         -- Execute Check ddl
         prc_set_utl_columns (l_sql_create);
         prc_store_ddl (
            'CONSTRAINT'
          ,    SUBSTR (
                  g_vc_nk_name_hist
                , 1
                , 25
               )
            || '_NN'
            || TO_CHAR (
                  i
                , '00'
               )
          , l_sql_create
         );

         BEGIN
            ddls.prc_create_object (
               'CONSTRAINT'
             ,    SUBSTR (
                     g_vc_nk_name_hist
                   , 1
                   , 25
                  )
               || '_NN'
               || TO_CHAR (
                     i
                   , '00'
                  )
             , l_sql_create
             , FALSE
             , TRUE
            );
         EXCEPTION
            WHEN OTHERS THEN
               trac.log_warn (
                  SQLERRM
                , 'Stage 2 Natural Key: Warning'
               );

               IF p_b_raise_flag THEN
                  RAISE;
               END IF;
         END;
      END LOOP;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_table_name_hist
         || ' TO '
         || stag_param.c_vc_list_grantee;

      trac.log_info (
         l_vc_message
       , 'History Table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         trac.log_info (
            SQLERRM
          , 'History Table: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_create_hist_table;

   PROCEDURE prc_create_hist_view (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   TYPE.vc_max_plsql := 'prc_create_hist_view';
      l_vc_message    VARCHAR2 (32000)
                         :=    'View stage 2 '
                            || g_vc_view_name_hist;
      l_sql_create    TYPE.vc_max_plsql;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Stage 2 View: Begin'
      );
      l_vc_viw_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_viewcols;
      --
      l_sql_create :=
            'CREATE OR REPLACE FORCE VIEW '
         || g_vc_view_name_hist
         || ' AS SELECT '
         || NVL (
               LTRIM (
                  l_vc_viw_anonymized
                , ','
               )
             , '*'
            )
         || ' FROM '
         || g_vc_table_name_hist;
      prc_store_ddl (
         'VIEW'
       , g_vc_view_name_hist
       , l_sql_create
      );

      EXECUTE IMMEDIATE l_sql_create;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_view_name_hist
         || ' TO '
         || stag_param.c_vc_list_grantee;

      trac.log_info (
         l_vc_message
       , 'Stage 2 View: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         trac.log_info (
            SQLERRM
          , 'Stage 2 View: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         ELSE
            NULL;
         END IF;
   END;

   PROCEDURE prc_create_hist_synonym (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   TYPE.vc_max_plsql := 'prc_create_hist_synonym';
      l_vc_message    VARCHAR2 (32000)
                         :=    'Synonym stage 2 '
                            || g_vc_view_name_hist;
      l_sql_create    TYPE.vc_max_plsql;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Stage 2 Synonym: Begin'
      );
      l_vc_viw_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_viewcols;
      --
      l_sql_create :=
            'CREATE OR REPLACE SYNONYM '
         || g_vc_view_name_hist
         || ' FOR '
         || g_vc_table_name_hist;
      prc_store_ddl (
         'SYNONYM'
       , g_vc_view_name_hist
       , l_sql_create
      );

      EXECUTE IMMEDIATE l_sql_create;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_view_name_hist
         || ' TO '
         || stag_param.c_vc_list_grantee;

      trac.log_info (
         l_vc_message
       , 'Stage 2 Synonym: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         trac.log_info (
            SQLERRM
          , 'Stage 2 Synonym: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         ELSE
            NULL;
         END IF;
   END;

   PROCEDURE prc_create_fbda_view (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   TYPE.vc_max_plsql := 'prc_create_fbda_view';
      l_vc_message    VARCHAR2 (32000)
                         :=    'View stage 2 '
                            || g_vc_view_name_hist;
      l_sql_create    TYPE.vc_max_plsql;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Stage 2 View: Begin'
      );
      l_vc_viw_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_viewcols;
      --
      l_sql_create :=
            'CREATE OR REPLACE FORCE VIEW '
         || g_vc_view_name_fbda
         || ' AS SELECT versions_starttime
     , versions_startscn
     , versions_endtime
     , versions_endscn
     , versions_xid
     , versions_operation
     '
         || l_vc_viw_anonymized
         || ' FROM '
         || g_vc_table_name_hist
         || ' VERSIONS BETWEEN TIMESTAMP MINVALUE AND MAXVALUE';
      prc_store_ddl (
         'VIEW'
       , g_vc_view_name_fbda
       , l_sql_create
      );

      EXECUTE IMMEDIATE l_sql_create;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_view_name_fbda
         || ' TO '
         || stag_param.c_vc_list_grantee;

      trac.log_info (
         l_vc_message
       , 'Stage 2 View: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         trac.log_info (
            SQLERRM
          , 'Stage 2 View: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         ELSE
            NULL;
         END IF;
   END;

   PROCEDURE prc_create_prc_trunc_stage (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      TYPE.vc_max_plsql := 'prc_create_prc_trunc_stage';
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure Trunc Stage table'
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- HEAD
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_trunc_stage
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      l_sql_prc_token := stmt.c_token_truncate_table;
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         l_sql_prc_token := stmt.c_token_truncate_table;
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_dupl
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END IF;

      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_trunc_stage
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_trunc_stage;

   PROCEDURE prc_create_prc_trunc_diff (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      TYPE.vc_max_plsql := 'prc_create_prc_trunc_diff';
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure Trunc diff table'
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- HEAD
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_trunc_diff
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      l_sql_prc_buffer := stmt.c_token_truncate_table;
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_diff
      );
      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_trunc_diff
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_trunc_diff;

   PROCEDURE prc_create_prc_init (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name         TYPE.vc_max_plsql := 'prc_create_prc_init';
      l_vc_message          VARCHAR2 (32000)
                               :=    'Procedure load init'
                                  || g_vc_package_main;
      l_sql_prc             CLOB;
      l_sql_prc_token       CLOB;
      l_sql_prc_buffer      CLOB;
      -- List of columns
      l_vc_col_all          TYPE.vc_max_plsql;
      l_list_utl_col        TYPE.vc_max_plsql;
      l_list_utl_val        TYPE.vc_max_plsql;
      l_list_utl_col_dupl   TYPE.vc_max_plsql;
      l_list_utl_val_dupl   TYPE.vc_max_plsql;
   BEGIN
      l_vc_col_anonymized := '';
      l_vc_fct_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_columns;
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- Set utl columns strings
      l_list_utl_col_dupl :=
         CASE
            WHEN g_l_distr_code.COUNT > 1 THEN
                  c_token_utl_column_source_db
               || ','
            WHEN g_vc_partition_expr IS NOT NULL THEN
                  c_token_utl_column_partition
               || ','
         END;
      prc_set_utl_columns (l_list_utl_col_dupl);
      l_list_utl_col :=
            c_token_utl_column_hist
         || ','
         || l_list_utl_col_dupl;
      prc_set_utl_columns (l_list_utl_col);
      --
      -- Get lists of columns
      l_vc_col_all :=
         dict.fct_get_column_subset (
            g_vc_dblink
          , g_vc_owner_src
          , g_vc_table_name_source
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
          , p_vc_exclude_list   => l_list_utl_col
         );
      --
      -- HEAD
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_init
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Add token to check if hist table is empty
      l_sql_prc_token :=
            stmt.c_token_enable_parallel_dml
         || CHR (10)
         || c_token_check_table_isempty;
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_hist
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Truncate duplicates table
         l_sql_prc_token := stmt.c_token_truncate_table;
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_dupl
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END IF;

      -- Fill stage hist for each source db
      FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
         l_sql_prc_token := c_token_stage_insert;
         --
         -- Values for the utility columns
         l_list_utl_val_dupl :=
            CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     ''''
                  || g_l_distr_code (i)
                  || ''', '
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     fct_get_partition_expr
                  || ','
            END;

         IF g_n_source_nk_flag = 0
        AND g_vc_col_pk_src IS NOT NULL THEN
            l_vc_col_pk_notnull :=
               stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_dedupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'duplIdentifier'
             , g_vc_table_name_dupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'pkColumnList'
             , g_vc_col_pk_src
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlColumnListForDupl'
             , l_list_utl_col_dupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlValueListForDupl'
             , l_list_utl_val_dupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
         ELSE
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_copy
            );
         END IF;

         l_list_utl_val :=
               c_token_utl_colval_hist
            || ','
            || l_list_utl_val_dupl;
         -- There is no optional incremental retrieval (this is an init procedure)
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'computeIncrementBound'
          , NULL
         );
         --
         --
         stmt.prc_set_text_param (
            l_sql_prc_token
          , 'targetIdentifier'
          , g_vc_table_name_hist
         );
         stmt.prc_set_text_param (
            l_sql_prc_token
          , 'sourceIdentifier'
          , g_vc_source_identifier
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourceColumnList'
          , g_vc_col_all
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetColumnList'
          , g_vc_col_all
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlColumnList'
          , l_list_utl_col
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlValueList'
          , l_list_utl_val
         );
         --
         --
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'filterClause'
          , CASE
               WHEN g_vc_filter_clause IS NOT NULL THEN
                     ' WHERE '
                  || g_vc_filter_clause
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || '_'
                  || g_l_distr_code (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partitionId'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END LOOP;

      l_sql_prc_token := c_token_analyze;
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'HSAN'
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_hist
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'partition'
       , NULL
      );
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Truncate duplicates table
         l_sql_prc_token := c_token_analyze;
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'statisticsType'
          , 'DUAN'
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_dupl
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'stgOwner'
          , g_vc_owner_stg
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , NULL
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END IF;

      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceCode'
       , g_vc_source_code
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'objectName'
       , g_vc_object_name
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceTable'
       , g_vc_table_name_source
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_init
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_init;

   PROCEDURE prc_create_prc_load_stage (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      TYPE.vc_max_plsql := 'prc_create_prc_load_stage';
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure load stage1 '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
      l_list_utl_col     TYPE.vc_max_plsql;
      l_list_utl_val     TYPE.vc_max_plsql;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- Set utl columns strings
      l_list_utl_col :=
         CASE
            WHEN g_l_distr_code.COUNT > 1 THEN
                  c_token_utl_column_source_db
               || ','
            WHEN g_vc_partition_expr IS NOT NULL THEN
                  c_token_utl_column_partition
               || ','
         END;
      prc_set_utl_columns (l_list_utl_col);
      --
      -- HEAD
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_stage
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Truncate stage table
      l_sql_prc_token :=
            stmt.c_token_enable_parallel_dml
         || CHR (10)
         || stmt.c_token_truncate_table;
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Truncate duplicates table
         l_sql_prc_token := stmt.c_token_truncate_table;
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_dupl
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END IF;

      -- Fill stage table for each source db
      -- Fill stage for each source db
      FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
         l_sql_prc_token := c_token_stage_insert;
         --
         -- Values for the utility columns
         l_list_utl_val :=
            CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     ''''
                  || g_l_distr_code (i)
                  || ''', '
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     fct_get_partition_expr
                  || ', '
            END;

         IF g_n_source_nk_flag = 0
        AND g_vc_col_pk_src IS NOT NULL THEN
            l_vc_col_pk_notnull :=
               stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_dedupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'duplIdentifier'
             , g_vc_table_name_dupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'pkColumnList'
             , g_vc_col_pk_src
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlColumnListForDupl'
             , l_list_utl_col
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlValueListForDupl'
             , l_list_utl_val
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
         ELSE
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_copy
            );
         END IF;

         -- Add optional increment retrieval statement
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'computeIncrementBound'
          , CASE
               WHEN g_vc_increment_column IS NOT NULL THEN
                  c_token_stage_get_incr_bound
            END
         );
         --
         --
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'insertStatement'
          , stmt.c_sql_insert_copy
         );
         stmt.prc_set_text_param (
            l_sql_prc_token
          , 'targetIdentifier'
          , g_vc_table_name_stage
         );
         stmt.prc_set_text_param (
            l_sql_prc_token
          , 'sourceIdentifier'
          , g_vc_source_identifier
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourceColumnList'
          , g_vc_col_all
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetColumnList'
          , g_vc_col_all
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlColumnList'
          , l_list_utl_col
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlValueList'
          , l_list_utl_val
         );
         --
         --
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'incrementColumn'
          , g_vc_increment_column
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'histTableName'
          , g_vc_table_name_hist
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'filterClause'
          ,    CASE
                  WHEN g_vc_filter_clause IS NOT NULL THEN
                        'WHERE '
                     || g_vc_filter_clause
               END
            || CASE
                  WHEN g_vc_increment_column IS NOT NULL THEN
                        CASE
                           WHEN g_vc_filter_clause IS NULL THEN
                              ' WHERE '
                           ELSE
                              ' AND '
                        END
                     || g_vc_increment_column
                     || ' > l_t_increment_bound - '
                     || NVL (g_n_increment_buffer, 0)
               END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || '_'
                  || g_l_distr_code (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partitionId'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END LOOP;

      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , CASE
            WHEN g_vc_increment_column IS NOT NULL THEN
                  'l_t_increment_bound '
               || g_vc_increment_coldef
               || ';'
         END
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceCode'
       , g_vc_source_code
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'objectName'
       , g_vc_object_name
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceTable'
       , g_vc_table_name_source
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_stage
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_load_stage;

   PROCEDURE prc_create_prc_load_stage_p (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      TYPE.vc_max_plsql := 'prc_create_prc_load_stage_p';
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure load stage1 partition '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
      l_n_iter_begin     NUMBER;
      l_n_iter_end       NUMBER;
      l_list_utl_col     TYPE.vc_max_plsql;
      l_list_utl_val     TYPE.vc_max_plsql;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- Set utl columns strings
      l_list_utl_col :=
         CASE
            WHEN g_l_distr_code.COUNT > 1 THEN
                  c_token_utl_column_source_db
               || ','
            WHEN g_vc_partition_expr IS NOT NULL THEN
                  c_token_utl_column_partition
               || ','
         END;
      prc_set_utl_columns (l_list_utl_col);

      --
      -- HEAD
      --
      IF g_l_distr_code.COUNT > 1 THEN
         FOR i IN g_l_dblink.FIRST .. g_l_dblink.LAST LOOP
            -- Stage1 procedure head
            l_sql_prc := ddls.c_template_prc_head;
            ddls.prc_set_text_param (
               l_sql_prc
             , 'prcName'
             ,    stag_param.c_vc_procedure_load_stage_p
               || '_'
               || g_l_distr_code (i)
            );
            ddls.prc_set_text_param (
               l_sql_prc
             , 'prcParameters'
             , c_token_prc_param
            );
            l_buffer_pkg_head :=
                  l_buffer_pkg_head
               || CHR (10)
               || l_sql_prc;
         END LOOP;
      ELSIF g_vc_partition_expr IS NOT NULL THEN
         FOR i IN 0 .. 9 LOOP
            -- Stage1 procedure head
            l_sql_prc := ddls.c_template_prc_head;
            ddls.prc_set_text_param (
               l_sql_prc
             , 'prcName'
             ,    stag_param.c_vc_procedure_load_stage_p
               || '_p'
               || i
            );
            ddls.prc_set_text_param (
               l_sql_prc
             , 'prcParameters'
             , c_token_prc_param
            );
            l_buffer_pkg_head :=
                  l_buffer_pkg_head
               || CHR (10)
               || l_sql_prc;
         END LOOP;
      END IF;

      --
      -- BODY
      --
      IF g_l_distr_code.COUNT > 1 THEN
         l_n_iter_begin := g_l_dblink.FIRST;
         l_n_iter_end := g_l_dblink.LAST;
      ELSIF g_vc_partition_expr IS NOT NULL THEN
         l_n_iter_begin := 0;
         l_n_iter_end := 9;
      END IF;

      FOR i IN l_n_iter_begin .. l_n_iter_end LOOP
         l_sql_prc_token := stmt.c_token_truncate_partition;
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_stage
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || '_'
                  || g_l_distr_code (i)
                  || ')'
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         -- Fill stage table for each source db
         l_sql_prc_token :=
               l_sql_prc_token
            || CHR (10)
            || c_token_stage_insert;
         --
         -- Values for the utility columns
         l_list_utl_val :=
            CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     ''''
                  || g_l_distr_code (i)
                  || ''', '
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     fct_get_partition_expr
                  || ', '
            END;

         IF g_n_source_nk_flag = 0
        AND g_vc_col_pk_src IS NOT NULL THEN
            l_vc_col_pk_notnull :=
               stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_dedupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'duplIdentifier'
             , g_vc_table_name_dupl
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'pkColumnList'
             , g_vc_col_pk_src
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlColumnListForDupl'
             , l_list_utl_col
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlValueListForDupl'
             , l_list_utl_val
            );
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
         ELSE
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_copy
            );
         END IF;

         -- Add optional increment retrieval statement
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'computeIncrementBound'
          , CASE
               WHEN g_vc_increment_column IS NOT NULL THEN
                  c_token_stage_get_incr_bound
            END
         );
         --
         --
         g_vc_source_identifier :=
            CASE
               WHEN g_l_dblink.COUNT = 1 THEN
                  CASE
                     WHEN g_l_dblink (1) IS NULL
                      AND g_l_owner_src (1) = g_vc_owner_stg THEN
                        g_vc_table_name_source
                     ELSE
                           CASE
                              WHEN g_l_owner_src (1) IS NOT NULL THEN
                                    g_l_owner_src (1)
                                 || '.'
                           END
                        || g_vc_table_name_source
                        || CASE
                              WHEN g_l_dblink (1) IS NOT NULL THEN
                                    '@'
                                 || g_l_dblink (1)
                           END
                  END
               ELSE
                  CASE
                     WHEN g_l_dblink (i) IS NULL
                      AND g_l_owner_src (i) = g_vc_owner_stg THEN
                        g_vc_table_name_source
                     ELSE
                        CASE
                           WHEN g_l_owner_src (i) IS NOT NULL THEN
                                 g_l_owner_src (i)
                              || '.'
                              || g_vc_table_name_source
                              || CASE
                                    WHEN g_l_dblink (i) IS NOT NULL THEN
                                          '@'
                                       || g_l_dblink (i)
                                 END
                        END
                  END
            END;
         --
         --
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'insertStatement'
          , stmt.c_sql_insert_copy
         );
         stmt.prc_set_text_param (
            l_sql_prc_token
          , 'targetIdentifier'
          , g_vc_table_name_stage
         );
         stmt.prc_set_text_param (
            l_sql_prc_token
          , 'sourceIdentifier'
          , g_vc_source_identifier
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourceColumnList'
          , g_vc_col_all
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetColumnList'
          , g_vc_col_all
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlColumnList'
          , l_list_utl_col
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlValueList'
          , l_list_utl_val
         );
         --
         --
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'incrementColumn'
          , g_vc_increment_column
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'filterClause'
          ,    CASE
                  WHEN g_vc_partition_expr IS NOT NULL THEN
                        ' WHERE '
                     || fct_get_partition_expr
                     || ' = '
                     || i
               END
            || CASE
                  WHEN g_vc_filter_clause IS NOT NULL THEN
                        CASE
                           WHEN g_vc_partition_expr IS NULL THEN
                              ' WHERE '
                           ELSE
                              ' AND '
                        END
                     || g_vc_filter_clause
               END
            || CASE
                  WHEN g_vc_increment_column IS NOT NULL THEN
                        CASE
                           WHEN g_vc_partition_expr IS NULL
                            AND g_vc_filter_clause IS NULL THEN
                              ' WHERE '
                           ELSE
                              ' AND '
                        END
                     || g_vc_increment_column
                     || ' > l_t_increment_bound'
               END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || '_'
                  || g_l_distr_code (i)
                  || ')'
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partitionId'
          , CASE
               WHEN g_l_distr_code.COUNT > 1
                 OR g_vc_partition_expr IS NOT NULL THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         -- Put body in the generic prc template
         l_sql_prc := ddls.c_template_prc_body;
         ddls.prc_set_text_param (
            l_sql_prc
          , 'prcParameters'
          , c_token_prc_param
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'varList'
          , CASE
               WHEN g_vc_increment_column IS NOT NULL THEN
                     'l_t_increment_bound '
                  || g_vc_increment_coldef
                  || ';'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'prcInitialize'
          , c_token_prc_initialize
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'prcFinalize'
          , c_token_prc_finalize
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'exceptionHandling'
          , c_token_prc_exception
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'prcBody'
          , l_sql_prc_token
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'sourceCode'
          , g_vc_source_code
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'objectName'
          , g_vc_object_name
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'sourceTable'
          , g_vc_table_name_source
         );
         ddls.prc_set_text_param (
            l_sql_prc
          , 'prcName'
          ,    stag_param.c_vc_procedure_load_stage_p
            || '_'
            || CASE
                  WHEN g_l_distr_code.COUNT > 1 THEN
                     g_l_distr_code (i)
                  ELSE
                        'p'
                     || i
               END
         );
         l_buffer_pkg_body :=
               l_buffer_pkg_body
            || CHR (10)
            || l_sql_prc;
      END LOOP;

      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_load_stage_p;

   PROCEDURE prc_create_prc_load_diff (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name         TYPE.vc_max_plsql := 'prc_create_prc_load_diff';
      l_vc_message          VARCHAR2 (32000)
                               :=    'Procedure load hist'
                                  || g_vc_package_main;
      l_sql_prc             CLOB;
      l_sql_prc_token       CLOB;
      l_sql_prc_buffer      CLOB;
      --
      l_n_iter_begin        NUMBER;
      l_n_iter_end          NUMBER;
      -- List of columns
      l_vc_col_all          TYPE.vc_max_plsql;
      l_vc_col_pk_hist      TYPE.vc_max_plsql;
      l_vc_clause_on        TYPE.vc_max_plsql;
      l_vc_upd_clause_set   TYPE.vc_max_plsql;
      l_vc_clause_update    TYPE.vc_max_plsql;
      l_vc_col_nvl2         TYPE.vc_max_plsql;
      -- Utl columns
      l_list_utl_col        TYPE.vc_max_plsql;
      l_list_utl_val        TYPE.vc_max_plsql;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- Set utl columns strings
      l_list_utl_col := c_token_utl_column_hist;
      -- Get list of pk columns of the History Table
      l_vc_col_pk_hist :=
         dict.fct_get_column_list (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'PK'
          , 'LIST_SIMPLE'
         );
      --
      -- HEAD
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_diff
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      -- Hist incremental procedure head
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_diff_incr
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Get list of all columns
      l_vc_col_all :=
         dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
         );                                                                                                                                                                -- In case the pk of stage 1 and History Tables is not the same, write a warning log

      IF g_vc_col_pk = l_vc_col_pk_hist
      OR (g_vc_col_pk IS NULL
      AND l_vc_col_pk_hist IS NULL) THEN
         trac.log_info (
               'Source '
            || g_vc_source_code
            || ', Object '
            || g_vc_table_name_source
            || ' : Stage and hist table have the same Natural Keys'
          , 'CHECK NK'
         );
      ELSE
         trac.log_info (
               'Source '
            || g_vc_source_code
            || ', Object '
            || g_vc_table_name_source
            || ' : Stage and hist table have different Natural Keys'
          , 'CHECK NK'
         );
      END IF;

      -- analyze stage table
      l_sql_prc_token := c_token_analyze;
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'STAN'
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'partition'
       , NULL
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Analyse duplicates table
         l_sql_prc_token := c_token_analyze;
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'statisticsType'
          , 'DUAN'
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'stgOwner'
          , g_vc_owner_stg
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_dupl
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , NULL
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END IF;

      -- Check hist/stage nk differences and truncate diff table
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || c_token_check_nk_equal;
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'stgOwner'
       , g_vc_owner_stg
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'stageTableName'
       , g_vc_table_name_stage
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffTableName'
       , g_vc_table_name_stage
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'histTableName'
       , g_vc_table_name_stage
      );

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NULL THEN
         -- If there is no natural key (tecnical PK) then use the alternate difference method
         l_vc_clause_on :=
            dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'COMMON_ALL'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
            );
      ELSE
         -- If there is a natural key (tecnical PK) and the full outer join method is specified,
         -- then use the merge template
         -- Get list of conditions for the on clause of the merge
         l_vc_clause_on :=
            dict.fct_get_column_list (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'PK'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
            );
         l_vc_col_nvl2 :=
            dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'COMMON_ALL'
             , 'LIST_NVL2'
             , 'src'
             , 'trg'
            );
         l_vc_clause_update :=
            dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'COMMON_NPK'
             , 'OR_DECODE'
             , 'trg'
             , 'src'
            );
      END IF;

      l_n_iter_begin := 0;

      IF g_vc_partition_expr IS NOT NULL THEN
         l_n_iter_end := 9;
      ELSE
         l_n_iter_end := 0;
      END IF;

      l_sql_prc_token := '';

      FOR i IN l_n_iter_begin .. l_n_iter_end LOOP
         l_sql_prc_token := c_token_diff_insert;

         IF g_n_source_nk_flag = 0
        AND g_vc_col_pk_src IS NULL THEN
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_diff_without_nk
            );
         ELSE
            ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , stmt.c_sql_insert_diff_with_nk
            );
         END IF;

         dict.prc_set_text_param (
            l_sql_prc_token
          , 'enableParallelDML'
          , CASE
               WHEN l_vc_set_anonymized IS NOT NULL THEN
                  stmt.c_token_enable_parallel_dml
               ELSE
                  stmt.c_token_disable_parallel_dml
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'diffPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourcePartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partitionId'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END LOOP;

      -- Set object identifiers
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffIdentifier'
       , g_vc_table_name_diff
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceIdentifier'
       , g_vc_table_name_stage
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetIdentifier'
       , g_vc_table_name_hist
      );
      -- Set list of columns
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'nvl2ColumnList'
       , l_vc_col_nvl2
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetColumnList'
       , l_vc_col_all
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'utlColumnList'
       , l_list_utl_col
      );
      -- Set clauses
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'updateClause'
       , l_vc_clause_update
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'joinClause'
       , l_vc_clause_on
      );
      prc_set_utl_columns (l_sql_prc_buffer);
      -- Ad analyze token
      l_sql_prc_token := c_token_analyze;
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'DFAN'
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_diff
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'partition'
       , NULL
      );
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || l_sql_prc_token;
      -- Put all other code parameters
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceCode'
       , g_vc_source_code
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'objectName'
       , g_vc_object_name
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceTable'
       , g_vc_table_name_source
      );
      --
      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'joinType'
       , 'FULL'
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'operationClause'
       , ' IS NOT NULL'
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_diff
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      --
      -- Load stage 2 without deletes
      --
      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'joinType'
       , 'LEFT'
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'operationClause'
       , ' <> ''D'''
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_diff_incr
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_load_diff;

   PROCEDURE prc_create_prc_load_hist (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name         TYPE.vc_max_plsql := 'prc_create_prc_load_hist';
      l_vc_message          VARCHAR2 (32000)
                               :=    'Procedure load diff-to-stage2 '
                                  || g_vc_package_main;
      l_sql_prc             CLOB;
      l_sql_prc_token       CLOB;
      l_sql_prc_buffer      CLOB;
      --
      l_n_iter_begin        NUMBER;
      l_n_iter_end          NUMBER;
      -- List of columns
      l_vc_col_simple       TYPE.vc_max_plsql;
      l_vc_clause_on        TYPE.vc_max_plsql;
      l_vc_upd_clause_set   TYPE.vc_max_plsql;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      -- Set anonymizad column lists
      l_vc_set_anonymized := '';
      l_vc_col_anonymized := '';
      l_vc_fct_anonymized := '';
      l_vc_ins_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_columns;
      --
      -- HEAD
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_hist
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Get list of all columns
      l_vc_col_simple :=
         dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
         );

      /*l_vc_ins_col_target :=
         dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'COMMON_ALL'
          , 'LIST_ALIAS'
          , 'trg'
         );*/
      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NULL THEN
         -- If there is no natural key (tecnical PK) then use the alternate difference method
         l_vc_clause_on :=
            dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'COMMON_ALL'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
            );
      ELSE
         -- If there is a natural key (tecnical PK) and the full outer join method is specified,
         -- then use the merge template
         -- Get list of conditions for the on clause of the merge
         l_vc_clause_on :=
            dict.fct_get_column_list (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'PK'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
            );
         l_vc_upd_clause_set :=
            dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'COMMON_NPK'
             , 'SET_ALIAS'
             , 'trg'
             , 'src'
            );
      END IF;

      l_n_iter_begin := 0;

      IF g_vc_partition_expr IS NOT NULL THEN
         l_n_iter_end := 9;
      ELSE
         l_n_iter_end := 0;
      END IF;

      l_sql_prc_token := '';

      FOR i IN l_n_iter_begin .. l_n_iter_end LOOP
         l_sql_prc_token := c_token_hist_reconcile;
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'updateStatement'
          , stmt.c_sql_reconcile_update
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'insertStatement'
          , stmt.c_sql_reconcile_insert
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'enableParallelDML'
          , CASE
               WHEN l_vc_set_anonymized IS NOT NULL THEN
                  stmt.c_token_enable_parallel_dml
               ELSE
                  stmt.c_token_disable_parallel_dml
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'diffPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'diffPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partitionId'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END LOOP;

      -- Set object identifiers
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffIdentifier'
       , g_vc_table_name_diff
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetIdentifier'
       , g_vc_table_name_hist
      );
      -- Set list of columns
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffColumnList'
       ,    l_vc_col_simple
         || CHR (10)
         || l_vc_ins_anonymized
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetColumnList'
       ,    l_vc_col_simple
         || CHR (10)
         || l_vc_col_anonymized
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'utlColumnList'
       , c_token_utl_column_hist
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'utlValueList'
       , c_token_utl_colval_hist
      );
      /* ddls.prc_set_text_param (
          l_sql_prc_token
        , 'listColSource'
        ,    l_vc_ins_col_source
          || CHR (10)
          || l_vc_ins_anonymized
       );
       ddls.prc_set_text_param (
          l_sql_prc_token
        , 'listColTarget'
        ,    l_vc_ins_col_target
          || CHR (10)
          || l_vc_col_anonymized
       );*/
      -- Set clauses
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'joinClause'
       , l_vc_clause_on
      );
      -- Set the matched clause of the merge statement. This exists only if there are non-NK columns to set
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'matchedClause'
       , CASE
            WHEN l_vc_upd_clause_set IS NOT NULL THEN
                  l_vc_upd_clause_set
               || CHR (10)
               || l_vc_set_anonymized
               || ', '
         END
      );
      prc_set_utl_columns (l_sql_prc_buffer);
      -- Analyze token
      l_sql_prc_token := c_token_analyze;
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'HSAN'
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_hist
      );
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || l_sql_prc_token;                                                                                                                                                                                                   -- Put all other code parameters
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceCode'
       , g_vc_source_code
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'objectName'
       , g_vc_object_name
      );
      --
      -- Load stage 2 with table comparison
      --
      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_load_hist
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_load_hist;

   PROCEDURE prc_create_prc_wrapper (
      p_b_tc_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name      TYPE.vc_max_plsql := 'prc_create_prc_wrapper';
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure wrapper '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- HEAD for FULL load
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_wrapper
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY for FULL load
      --
      l_sql_prc_buffer := c_token_prc_wrapper;

      IF p_b_tc_only_flag THEN
         ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          , NULL
         );
      ELSE
         ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          ,    stag_param.c_vc_procedure_load_stage
            || ';'
         );
      END IF;

      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadDiff'
       ,    stag_param.c_vc_procedure_load_diff
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadHist'
       ,    stag_param.c_vc_procedure_load_hist
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncStage'
       ,    stag_param.c_vc_procedure_trunc_stage
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncDiff'
       ,    stag_param.c_vc_procedure_trunc_diff
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_hist
      );
      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_wrapper
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      --
      -- HEAD for INCREMENTAL load
      --
      l_sql_prc := ddls.c_template_prc_head;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_wrapper_incr
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY for DELTA load
      --
      l_sql_prc_buffer := c_token_prc_wrapper;

      IF p_b_tc_only_flag THEN
         ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          , NULL
         );
      ELSE
         ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          ,    stag_param.c_vc_procedure_load_stage
            || ';'
         );
      END IF;

      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadDiff'
       ,    stag_param.c_vc_procedure_load_diff
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadHist'
       ,    stag_param.c_vc_procedure_load_diff_incr
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncStage'
       ,    stag_param.c_vc_procedure_trunc_stage
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncDiff'
       ,    stag_param.c_vc_procedure_trunc_diff
         || ';'
      );
      ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_hist
      );
      -- Put body in the generic prc template
      l_sql_prc := ddls.c_template_prc_body;
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , NULL
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , stag_param.c_vc_procedure_wrapper_incr
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_wrapper;

   PROCEDURE prc_compile_package_main (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   TYPE.vc_max_plsql := 'prc_compile_package_main';
      l_vc_message    VARCHAR2 (32000)
                         :=    'Package compile '
                            || g_vc_package_main;
      l_sql_create    CLOB;
   BEGIN
      -- Package head
      trac.log_info (
         l_vc_message
       , 'Package head: Begin'
      );
      l_sql_create := ddls.c_template_pkg_head;
      ddls.prc_set_text_param (
         l_sql_create
       , 'pkgName'
       , g_vc_package_main
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'varList'
       , ''
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'prcList'
       , l_buffer_pkg_head
      );
      -- Execute ddl for package head
      prc_store_ddl (
         'PACKAGE'
       , g_vc_package_main
       , l_sql_create
      );
      ddls.prc_create_object (
         'PACKAGE'
       , g_vc_package_main
       , l_sql_create
       , FALSE
       , p_b_raise_flag
      );
      trac.log_info (
         l_vc_message
       , 'Package head: End'
      );
      -- Package body
      trac.log_info (
         l_vc_message
       , 'Package body: Begin'
      );
      l_sql_create := ddls.c_template_pkg_body;
      ddls.prc_set_text_param (
         l_sql_create
       , 'varList'
       , ''
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'prcList'
       , l_buffer_pkg_body
      );
      ddls.prc_set_text_param (
         l_sql_create
       , 'pkgName'
       , g_vc_package_main
      );
      -- Execute ddl for package body
      prc_store_ddl (
         'PACKAGE BODY'
       , g_vc_package_main
       , l_sql_create
      );
      ddls.prc_create_object (
         'PACKAGE BODY'
       , g_vc_package_main
       , l_sql_create
       , FALSE
       , p_b_raise_flag
      );
      trac.log_info (
         l_vc_message
       , 'Package body: End'
      );
   END prc_compile_package_main;

   PROCEDURE prc_create_package_main (
      p_b_hist_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag        BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name   TYPE.vc_max_plsql := 'prc_create_package_main';
      l_vc_message    VARCHAR2 (32000)
                         :=    'Package create '
                            || g_vc_package_main;
      l_sql_create    CLOB;
   BEGIN
      trac.log_info (
         l_vc_message
       , 'Begin'
      );
      l_buffer_pkg_head := '';
      l_buffer_pkg_body := '';

      IF NOT p_b_hist_only_flag THEN
         -- Get list of columns for the stage 1 and init procedures
         l_vc_col_src :=
            dict.fct_get_column_list (
               g_vc_dblink
             , g_vc_owner_src
             , g_vc_table_name_source
             , 'ALL'
             , 'LIST_SIMPLE'
            );
         l_vc_col_dupl :=
            dict.fct_get_column_subset (
               g_vc_dblink
             , g_vc_owner_src
             , g_vc_table_name_source
             , g_vc_owner_stg
             , g_vc_table_name_dupl
             , 'COMMON_ALL'
             , 'LIST_SIMPLE'
            );
      END IF;

      --
      -- Fill buffers with single procedures
      --
      -- Trunc Stage Table
      prc_create_prc_trunc_stage (p_b_raise_flag);
      --
      -- Trunc Diff table
      prc_create_prc_trunc_diff (p_b_raise_flag);

      IF NOT p_b_hist_only_flag THEN
         --
         -- Initial load
         prc_create_prc_init (p_b_raise_flag);
         --
         -- Stage 1 load
         prc_create_prc_load_stage (p_b_raise_flag);

         IF g_l_dblink.COUNT > 1
         OR g_vc_partition_expr IS NOT NULL THEN
            --
            -- Stage 1 load - single partitions
            prc_create_prc_load_stage_p (p_b_raise_flag);
         END IF;
      END IF;

      --
      -- Stage 2 load
      prc_create_prc_load_diff (p_b_raise_flag);
      prc_create_prc_load_hist (p_b_raise_flag);
      --
      -- Wrapper
      prc_create_prc_wrapper (
         p_b_hist_only_flag
       , p_b_raise_flag
      );
      --
      -- Compile package
      prc_compile_package_main (p_b_raise_flag);
      trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_package_main;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: $';
   c_body_url := '$HeadURL: $';
END stag_ddl;