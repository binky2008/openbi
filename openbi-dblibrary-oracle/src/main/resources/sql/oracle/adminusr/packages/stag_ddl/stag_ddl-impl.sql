CREATE OR REPLACE PACKAGE BODY p#frm#stag_ddl
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: stag_ddl-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_ddl/stag_ddl-impl.sql $
   */
   /**
   * Templates for standard code tokens
   **/
   --
   c_token_utl_column_hist        t_string := '#validFromColumnName#, #validToColumnName#, #dmlOpColumnName#';
   c_token_utl_coldef_hist        t_string := '#validFromColumnName# DATE, #validToColumnName# DATE, #dmlOpColumnName# VARCHAR2(10)';
   c_token_utl_colval_hist        t_string := 'SYSDATE, TO_DATE(''99991231'',''yyyymmdd''), ''I''';
   c_token_utl_column_source_db   t_string := '#sourceDbColumnName#';
   c_token_utl_coldef_source_db   t_string := '#sourceDbColumnName# VARCHAR(100)';
   c_token_utl_column_partition   t_string := '#partitionColumnName#';
   c_token_utl_coldef_partition   t_string := '#partitionColumnName# NUMBER(1)';
   --
   c_token_diff_partition         CLOB
                                     :=    'PARTITION BY LIST ('
                                        || p#frm#stag_param.c_vc_column_dml_op
                                        || ')
    (  
	  PARTITION PI VALUES (''I'') NOLOGGING NOCOMPRESS
    , PARTITION PH VALUES (''H'') NOLOGGING NOCOMPRESS
    , PARTITION PU VALUES (''U'') NOLOGGING NOCOMPRESS
    , PARTITION PD VALUES (''D'') NOLOGGING NOCOMPRESS
    , PARTITION PR VALUES (''R'') NOLOGGING NOCOMPRESS
	)';
   c_token_diff_subpartition      CLOB
                                     :=    'PARTITION BY LIST (#partitionColumnName#)
    SUBPARTITION BY LIST ('
                                        || p#frm#stag_param.c_vc_column_dml_op
                                        || ')
    SUBPARTITION TEMPLATE 
    (  
        SUBPARTITION PI VALUES (''I''),
        SUBPARTITION PH VALUES (''H''),
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
   c_token_prc_exception          CLOB := 'p#frm#stag_stat.prc_stat_end(l_n_stat_id, 0, 1);';
   -- Standard parameters for a generated procedure
   c_token_prc_param              CLOB := 'p_n_stream NUMBER DEFAULT NULL';
   -- Code body for the wrapper procedure
   c_token_prc_wrapper            CLOB := '
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''Staging Begin'', ''Start extracting from #tableName#'');

		#prcLoadStage#

        #prcLoadDiff#

		#prcLoadHist#

		#prcTruncStage#

		#prcTruncDiff#

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''Staging End'', ''Stage completed for #tableName#'');';
   -- Statistics token
   c_token_analyze                CLOB := '
        l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', NULL, ''#statisticsType#'');
        
        DBMS_STATS.UNLOCK_TABLE_STATS (''#stgOwner#'', ''#tableName#'') ;
        DBMS_STATS.GATHER_TABLE_STATS (''#stgOwner#'', ''#tableName#'', NULL, 1);
        p#frm#stag_stat.prc_size_store(''#sourceCode#'', ''#objectName#'', ''#tableName#'');

        p#frm#stag_stat.prc_stat_end(l_n_stat_id, 0);

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''STAT END'', ''#tableName# : Statistics gathered'');
        ';
   -- Check token of the init procedure
   c_token_check_table_isempty    CLOB := '
		  p#frm#trac.log_sub_debug (l_vc_prc_name, ''CHECK'', ''Check table #tableName# '');
        SELECT COUNT (*)
          INTO l_n_result
          FROM #tableName#
         WHERE rownum = 1;
         
        IF l_n_result = 0 THEN
              p#frm#trac.log_sub_debug (l_vc_prc_name, ''CHECK'', ''Table #tableName# is empty'');
        ELSE
            p#frm#trac.log_sub_error (l_vc_prc_name, ''CHECK'', ''Table #tableName# is not empty'');
            raise_application_error (-20000, ''Cannot init load non-empty table'');        
        END IF;';
   -- Insert token of the staging 1 procedure
   c_token_stage_get_incr_bound   CLOB := '
   
          p#frm#trac.log_sub_debug (l_vc_prc_name, ''INCR BOUND'', ''#tableName# #partition# : get last #incrementColumn#'');
   
        SELECT MAX(#incrementColumn#)
          INTO l_t_increment_bound
          FROM #histTableName# #partition#;
          
          p#frm#trac.log_sub_debug (l_vc_prc_name, ''INCR BOUND'', ''#tableName# #partition# : last #incrementColumn# = '' || l_t_increment_bound);
        
        ';
   -- Insert token of the staging procedure
   c_token_stage_insert           CLOB := '
        l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''STIN'');

        #computeIncrementBound#
                   
        #insertStatement#

		l_n_result := SQL%ROWCOUNT;

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

        COMMIT;

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''INSERT END'', ''#targetIdentifier# #partition# : '' || l_n_result || '' rows inserted'', NULL, l_n_result);
		';
   -- Check token of the historicizing procedure
   c_token_diff_check             CLOB := '
        l_b_ok := p#frm#dict.fct_check_pk (
			NULL, ''#stgOwner#'', ''#stageTableName#'', ''#stgOwner#'', ''#histTableName#''
		);
		IF l_b_ok THEN
			  p#frm#trac.log_sub_debug (l_vc_prc_name, ''CHECK NK'', ''#stageTableName# and #histTableName# have the same NK'');
		ELSE
			  p#frm#trac.log_sub_warn (l_vc_prc_name, ''CHECK NK'', ''#stageTableName# and #histTableName# have not the same NK'');		
		END IF;
        
        SELECT COUNT(*) INTO l_n_result FROM #stageTableName#;
        
        IF l_n_result = 0 THEN
            p#frm#trac.log_sub_error (l_vc_prc_name, ''CHECK'', ''Table #stageTableName# is empty'');
            raise_application_error (-20000, ''Stage table is empty.'');        
        END IF;
        
        EXECUTE IMMEDIATE ''ALTER SESSION ENABLE PARALLEL DML'';
		
		-- Truncate Diff table
		 p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF TRUNCATE'', ''Truncate #diffIdentifier#'');		
		EXECUTE IMMEDIATE ''TRUNCATE TABLE #diffIdentifier# DROP STORAGE'';
		 p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF TRUNCATE'', ''#diffIdentifier# truncated'');
		';
   -- Diff token of the historicizing procedure - with nk
   c_token_diff_insert            CLOB := '
		p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF BEGIN'', ''Insert into #diffIdentifier#'');

		l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''DFIN'');
      
        #insertStatement#
        
		l_n_result := SQL%ROWCOUNT;

      COMMIT;

	  p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);
		
      p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF INSERTED'', ''#diffIdentifier# : '' || l_n_result || '' rows inserted'');
';
   -- Merge token of the historicizing procedure - 2 separate statement
   c_token_hist_reconcile         CLOB := '
        #enableParallelDML#
		
        -- Close old and deleted records in hist table
        
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''HIST CLOSE'', ''Update #targetIdentifier#'');
        l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''HSCL'');

        #closeStatement#

        l_n_result := SQL%ROWCOUNT;

        p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

        COMMIT;
        
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''HIST CLOSED'', ''#targetIdentifier# : '' || l_n_result || '' rows updated'');
        
    	-- Update Hist table
		
		p#frm#trac.log_sub_debug (l_vc_prc_name, ''HIST UPDATE'', ''Update #targetIdentifier#'');
		l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''HSUP'');

        #updateStatement#

		l_n_result := SQL%ROWCOUNT;

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

        COMMIT;
		
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''HIST UPDATED'', ''#targetIdentifier# : '' || l_n_result || '' rows updated'');
		
		-- Insert into Hist table
		
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''HIST INSERT'', ''#targetIdentifier# : Insert'');

	    l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', #partitionId#, ''HSIN'');
        
        #insertStatement#

        l_n_result := SQL%ROWCOUNT;

        p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

	    COMMIT;

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''HIST END'', ''#targetIdentifier# : '' || l_n_result || '' rows inserted'');';
   -- Buffers
   l_buffer_pkg_head              CLOB;
   l_buffer_pkg_body              CLOB;
   l_vc_col_src                   t_string;
   l_vc_col_dupl                  t_string;
   l_vc_col_pk_notnull            t_string;
   -- Anonymization
   l_vc_def_anonymized            t_string;
   l_vc_col_anonymized            t_string;
   l_vc_set_anonymized            t_string;
   l_vc_ins_anonymized            t_string;
   l_vc_fct_anonymized            t_string;
   l_vc_ini_anonymized            t_string;
   l_vc_viw_anonymized            t_string;

   FUNCTION fct_get_partition_db (p_vc_db_identifier VARCHAR2)
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN    CHR (10)
             || ' PARTITION '
             || p#frm#stag_param.c_vc_prefix_partition
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
      l_vc_prc_name   t_object_name := 'prc_set_utl_columns';
   BEGIN
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'validFromColumnName'
       , p#frm#stag_param.c_vc_column_valid_from
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'validToColumnName'
       , p#frm#stag_param.c_vc_column_valid_to
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'dmlOpColumnName'
       , p#frm#stag_param.c_vc_column_dml_op
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'sourceDbColumnName'
       , p#frm#stag_param.c_vc_column_source_db
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'partitionColumnName'
       , p#frm#stag_param.c_vc_column_partition
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
      l_vc_prc_name   t_object_name := 'prc_store_ddl';
   BEGIN
      MERGE INTO p#frm#stag_ddl_t trg
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
      l_vc_prc_name    t_object_name := 'prc_create_stage_table';
      l_vc_message     t_string
                          :=    'Stage Table '
                             || g_vc_table_name_stage;
      l_sql_create     CLOB;
      l_list_utl_col   t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
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
      l_sql_create := p#frm#ddls.c_template_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_stage
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       , g_vc_col_def
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Creating table'
         );
         p#frm#ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_stage
          , l_sql_create
          , p_b_drop_flag
          , TRUE
         );
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Table created'
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'Error creating'
            );
            RAISE;
      END;

      BEGIN
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Setting compression option...'
         );

         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_stage
            || ' COMPRESS FOR QUERY LOW';

         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Compression option set'
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'FOR QUERY LOW option not available'
            );
      END;

      -- Build constraint statement
      /*l_sql_create          := c_token_create_pk;
      p#frm#ddls.prc_set_text_param (l_sql_create
                                    , 'tableName'
                                    , g_vc_table_name_stage
                                     );
      p#frm#ddls.prc_set_text_param (l_sql_create
                                    , 'pkName'
                                    , g_vc_nk_name_stage
                                     );
      p#frm#ddls.prc_set_text_param (l_sql_create
                                    , 'listColPk'
                                    , g_vc_col_pk
                                     );
      p#frm#ddls.prc_set_text_param (l_sql_create
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
           p#frm#trac.log_sub_debug (l_vc_message, 'Creating NK...');
         p#frm#ddls.prc_create_object ('CONSTRAINT'
                                      , g_vc_nk_name_stage
                                      , l_sql_create
                                      , p_b_drop_flag
                                      , TRUE
                                       );
           p#frm#trac.log_sub_debug (l_vc_message, 'NK created');
      EXCEPTION
         WHEN OTHERS
         THEN
              p#frm#trac.log_sub_debug (SQLERRM
                           , 'NK not created'
                           , param.gc_log_warn
                            );
            RAISE;
      END;*/
      IF g_n_parallel_degree > 1 THEN
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Setting parallel option...'
         );

         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_stage
            || ' PARALLEL '
            || g_n_parallel_degree;

         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Parallel option set...'
         );
      END IF;

      -- Comments from source system
      p#frm#trac.log_sub_debug (
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
                       FROM p#frm#stag_object_t o
                          , p#frm#stag_column_t c
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

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Comments set...'
      );
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_sub_error (
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
      l_vc_prc_name    t_object_name := 'prc_create_duplicate_table';
      l_vc_message     t_string
                          :=    'Table duplicates '
                             || g_vc_table_name_dupl;
      l_sql_create     CLOB;
      l_list_utl_col   t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
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
      l_sql_create := p#frm#ddls.c_template_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_dupl
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       , g_vc_col_def
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_dupl
          , l_sql_create
          , p_b_drop_flag
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'Duplicates Table: Warning'
            );
            RAISE;
      END;

      IF g_n_parallel_degree > 1 THEN
         l_sql_create :=
               'ALTER TABLE '
            || g_vc_table_name_dupl
            || ' PARALLEL '
            || g_n_parallel_degree;
         p#frm#ddls.prc_execute (l_sql_create);
      END IF;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Duplicates Table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_sub_error (
            l_vc_prc_name
          , l_vc_message
          , 'Stage Table: Error'
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
      l_vc_prc_name            t_object_name := 'prc_create_diff_table';
      l_vc_message             t_string
                                  :=    'Table difference '
                                     || g_vc_table_name_diff;
      l_sql_create             CLOB;
      l_sql_subpart_template   t_string;
      l_list_utl_col           t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
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
      l_sql_create := p#frm#ddls.c_template_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       , g_vc_col_def
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_diff
          , l_sql_create
          , p_b_drop_flag
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'Difference Table: Error'
            );
            RAISE;
      END;

      l_sql_create := p#frm#ddls.c_template_create_pk;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'pkName'
       , g_vc_nk_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColPk'
       , g_vc_col_pk
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_create_object (
            'CONSTRAINT'
          , g_vc_table_name_diff
          , l_sql_create
          , p_b_drop_flag
          , p_b_raise_flag
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'Difference table: Warning'
            );
            RAISE;
      END;

      IF g_n_parallel_degree > 1 THEN
         l_sql_create :=
               'ALTER TABLE '
            || g_vc_table_name_diff
            || ' PARALLEL '
            || g_n_parallel_degree;
         p#frm#ddls.prc_execute (l_sql_create);
      END IF;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Difference table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_sub_error (
            l_vc_prc_name
          , l_vc_message
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
      l_vc_prc_name     t_object_name := 'prc_create_hist_table';
      l_vc_message      t_string
                           :=    'History Table '
                              || g_vc_table_name_hist;
      l_sql_create      t_string;
      l_list_utl_col    t_string;
      l_l_utl_columns   DBMS_SQL.varchar2s;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
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
      l_sql_create := p#frm#ddls.c_template_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_hist
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_list_utl_col
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColumns'
       ,    g_vc_col_def
         || l_vc_def_anonymized
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_hist
          , l_sql_create
          , FALSE
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_sub_error (
               l_vc_prc_name
             , l_vc_message
             , 'History Table Create: Warning'
            );

            IF l_vc_def_anonymized IS NOT NULL THEN
               BEGIN
                  p#frm#trac.log_sub_debug (
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
                     p#frm#trac.log_sub_warn (
                        l_vc_prc_name
                      , l_vc_message
                      , 'History Table Add Anonymized: Warning'
                     );

                     IF p_b_raise_flag THEN
                        RAISE;
                     END IF;
               END;
            END IF;

            IF l_vc_ini_anonymized IS NOT NULL THEN
               BEGIN
                  p#frm#trac.log_sub_debug (
                     l_vc_prc_name
                   , l_vc_message
                   , 'Fill new anonymized columns - History Table Upd Anonymized'
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
                     p#frm#trac.log_sub_warn (
                        l_vc_prc_name
                      , l_vc_message
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
         p#frm#ddls.prc_execute (l_sql_create);
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
               p#frm#trac.log_sub_debug (
                  l_vc_prc_name
                , l_vc_message
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
            p#frm#trac.log_sub_warn (
               l_vc_prc_name
             , l_vc_message
             , 'FOR QUERY LOW option not available'
            );
      END;

      -- Generate NK ddl
      l_sql_create := p#frm#ddls.c_template_create_pk;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_hist
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'pkName'
       , g_vc_nk_name_hist
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColPk'
       ,    p#frm#stag_param.c_vc_column_valid_to
         || ','
         || g_vc_col_pk
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'storageClause'
       ,    'NOLOGGING '
         || CASE
               WHEN g_l_distr_code.COUNT > 1
                AND p#frm#dict.fct_check_part (
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
         p#frm#ddls.prc_create_object (
            'CONSTRAINT'
          , g_vc_nk_name_hist
          , l_sql_create
          , FALSE
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_sub_warn (
               l_vc_prc_name
             , l_vc_message
             , 'Hist table Natural Key: Warning'
            );

            IF p_b_raise_flag THEN
               RAISE;
            END IF;
      END;

      -- Create not null constraints
      l_l_utl_columns :=
         p#frm#type.fct_string_to_list (
            c_token_utl_column_hist
          , ','
         );

      FOR i IN l_l_utl_columns.FIRST .. l_l_utl_columns.LAST LOOP
         l_sql_create := p#frm#ddls.c_template_create_notnull;
         p#frm#ddls.prc_set_text_param (
            l_sql_create
          , 'tableName'
          , g_vc_table_name_hist
         );
         p#frm#ddls.prc_set_text_param (
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
            p#frm#ddls.prc_create_object (
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
               p#frm#trac.log_warn (
                  SQLERRM
                , 'Hist Natural Key: Warning'
               );

               IF p_b_raise_flag THEN
                  RAISE;
               END IF;
         END;
      END LOOP;

      /*EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_table_name_hist
         || ' TO '
         || p#frm#stag_param.c_vc_list_grantee;*/
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'History Table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_sub_warn (
            l_vc_prc_name
          , l_vc_message
          , 'History Table: Warning'
         );

         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_create_hist_table;

   PROCEDURE prc_create_hist_view (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   t_object_name := 'prc_create_hist_view';
      l_vc_message    t_string
                         :=    'View Hist '
                            || g_vc_view_name_hist;
      l_sql_create    t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Hist View: Begin'
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
         || p#frm#stag_param.c_vc_list_grantee;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Hist View: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          , 'Hist View: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         ELSE
            NULL;
         END IF;
   END;

   PROCEDURE prc_create_hist_synonym (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   t_object_name := 'prc_create_hist_synonym';
      l_vc_message    t_string
                         :=    'Synonym Hist '
                            || g_vc_view_name_hist;
      l_sql_create    t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Hist Synonym: Begin'
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
         || p#frm#stag_param.c_vc_list_grantee;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Hist Synonym: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_sub_warn (
            l_vc_prc_name
          , l_vc_message
          , 'Hist Synonym: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         ELSE
            NULL;
         END IF;
   END;

   PROCEDURE prc_create_fbda_view (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   t_object_name := 'prc_create_fbda_view';
      l_vc_message    t_string
                         :=    'View Hist '
                            || g_vc_view_name_hist;
      l_sql_create    t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Hist View: Begin'
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
         || p#frm#stag_param.c_vc_list_grantee;

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Hist View: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_sub_warn (
            l_vc_prc_name
          , l_vc_message
          , 'Hist View: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         ELSE
            NULL;
         END IF;
   END;

   PROCEDURE prc_create_prc_trunc_stage (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      t_object_name := 'prc_create_prc_trunc_stage';
      l_vc_message       t_string
                            :=    'Procedure trunc stage '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Begin'
      );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_trunc_stage
      );
      p#frm#ddls.prc_set_text_param (
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
      l_sql_prc_token := p#frm#stmt.c_token_truncate_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         l_sql_prc_token := p#frm#stmt.c_token_truncate_table;
         p#frm#ddls.prc_set_text_param (
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
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_trunc_stage
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_trunc_stage;

   PROCEDURE prc_create_prc_trunc_diff (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      t_object_name := 'prc_create_prc_trunc_diff';
      l_vc_message       t_string
                            :=    'Procedure trunc diff '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Begin'
      );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_trunc_diff
      );
      p#frm#ddls.prc_set_text_param (
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
      l_sql_prc_buffer := p#frm#stmt.c_token_truncate_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_diff
      );
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_trunc_diff
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_trunc_diff;

   PROCEDURE prc_create_prc_init (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name         t_object_name := 'prc_create_prc_init';
      l_vc_message          t_string
                               :=    'Procedure load init '
                                  || g_vc_package_main;
      l_sql_prc             CLOB;
      l_sql_prc_token       CLOB;
      l_sql_prc_buffer      CLOB;
      -- List of columns
      l_vc_col_all          t_string;
      l_list_utl_col        t_string;
      l_list_utl_val        t_string;
      l_list_utl_col_dupl   t_string;
      l_list_utl_val_dupl   t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Begin'
      );
      l_vc_col_anonymized := '';
      l_vc_fct_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_columns;
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
         p#frm#dict.fct_get_column_subset (
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
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_init
      );
      p#frm#ddls.prc_set_text_param (
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
            p#frm#stmt.c_token_enable_parallel_dml
         || CHR (10)
         || c_token_check_table_isempty;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_hist
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Truncate duplicates table
         l_sql_prc_token := p#frm#stmt.c_token_truncate_table;
         p#frm#ddls.prc_set_text_param (
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
               p#frm#stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_dedupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'duplIdentifier'
             , g_vc_table_name_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'pkColumnList'
             , g_vc_col_pk_src
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlColumnListForDupl'
             , l_list_utl_col_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlValueListForDupl'
             , l_list_utl_val_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
         ELSE
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_copy
            );
         END IF;

         l_list_utl_val :=
               c_token_utl_colval_hist
            || ','
            || l_list_utl_val_dupl;
         -- There is no optional incremental retrieval (this is an init procedure)
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'computeIncrementBound'
          , NULL
         );
         --
         --
         p#frm#stmt.prc_set_text_param (
            l_sql_prc_token
          , 'targetIdentifier'
          , g_vc_table_name_hist
         );
         p#frm#stmt.prc_set_text_param (
            l_sql_prc_token
          , 'sourceIdentifier'
          , g_vc_source_identifier
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourceColumnList'
          , l_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetColumnList'
          , l_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlColumnList'
          , l_list_utl_col
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlValueList'
          , l_list_utl_val
         );
         --
         --
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'filterClause'
          , CASE
               WHEN g_vc_filter_clause IS NOT NULL THEN
                     ' WHERE '
                  || g_vc_filter_clause
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
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
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'HSAN'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_hist
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'statisticsType'
          , 'DUAN'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_dupl
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'stgOwner'
          , g_vc_owner_stg
         );
         p#frm#ddls.prc_set_text_param (
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
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceCode'
       , g_vc_source_code
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'objectName'
       , g_vc_object_name
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceTable'
       , g_vc_table_name_source
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_init
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_init;

   PROCEDURE prc_create_prc_load_stage (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      t_object_name := 'prc_create_prc_load_stage';
      l_vc_message       t_string
                            :=    'Procedure load stage '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
      l_list_utl_col     t_string;
      l_list_utl_val     t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
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
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_stage
      );
      p#frm#ddls.prc_set_text_param (
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
            p#frm#stmt.c_token_enable_parallel_dml
         || CHR (10)
         || p#frm#stmt.c_token_truncate_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Truncate duplicates table
         l_sql_prc_token := p#frm#stmt.c_token_truncate_table;
         p#frm#ddls.prc_set_text_param (
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
               p#frm#stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_dedupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'duplIdentifier'
             , g_vc_table_name_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'pkColumnList'
             , g_vc_col_pk_src
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlColumnListForDupl'
             , l_list_utl_col
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlValueListForDupl'
             , l_list_utl_val
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
         ELSE
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_copy
            );
         END IF;

         -- Add optional increment retrieval statement
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'computeIncrementBound'
          , CASE
               WHEN g_vc_increment_column IS NOT NULL THEN
                  c_token_stage_get_incr_bound
            END
         );
         --
         --
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'insertStatement'
          , p#frm#stmt.c_sql_insert_copy
         );
         p#frm#stmt.prc_set_text_param (
            l_sql_prc_token
          , 'targetIdentifier'
          , g_vc_table_name_stage
         );
         p#frm#stmt.prc_set_text_param (
            l_sql_prc_token
          , 'sourceIdentifier'
          , g_vc_source_identifier
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourceColumnList'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetColumnList'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlColumnList'
          , l_list_utl_col
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlValueList'
          , l_list_utl_val
         );
         --
         --
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'incrementColumn'
          , g_vc_increment_column
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'histTableName'
          , g_vc_table_name_hist
         );
         p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || '_'
                  || g_l_distr_code (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
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
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , CASE
            WHEN g_vc_increment_column IS NOT NULL THEN
                  'l_t_increment_bound '
               || g_vc_increment_coldef
               || ';'
         END
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceCode'
       , g_vc_source_code
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'objectName'
       , g_vc_object_name
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'sourceTable'
       , g_vc_table_name_source
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_stage
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_load_stage;

   PROCEDURE prc_create_prc_load_stage_p (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name      t_object_name := 'prc_create_prc_load_stage_p';
      l_vc_message       t_string
                            :=    'Procedure load stage partition '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
      l_n_iter_begin     NUMBER;
      l_n_iter_end       NUMBER;
      l_list_utl_col     t_string;
      l_list_utl_val     t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
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
            l_sql_prc := p#frm#ddls.c_template_prc_head;
            p#frm#ddls.prc_set_text_param (
               l_sql_prc
             , 'prcName'
             ,    p#frm#stag_param.c_vc_procedure_load_stage_p
               || '_'
               || g_l_distr_code (i)
            );
            p#frm#ddls.prc_set_text_param (
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
            l_sql_prc := p#frm#ddls.c_template_prc_head;
            p#frm#ddls.prc_set_text_param (
               l_sql_prc
             , 'prcName'
             ,    p#frm#stag_param.c_vc_procedure_load_stage_p
               || '_p'
               || i
            );
            p#frm#ddls.prc_set_text_param (
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
         l_sql_prc_token := p#frm#stmt.c_token_truncate_partition;
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_stage
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || '_'
                  || g_l_distr_code (i)
                  || ')'
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
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
               p#frm#stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_dedupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'duplIdentifier'
             , g_vc_table_name_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'pkColumnList'
             , g_vc_col_pk_src
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlColumnListForDupl'
             , l_list_utl_col
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'utlValueListForDupl'
             , l_list_utl_val
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
         ELSE
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_copy
            );
         END IF;

         -- Add optional increment retrieval statement
         p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'insertStatement'
          , p#frm#stmt.c_sql_insert_copy
         );
         p#frm#stmt.prc_set_text_param (
            l_sql_prc_token
          , 'targetIdentifier'
          , g_vc_table_name_stage
         );
         p#frm#stmt.prc_set_text_param (
            l_sql_prc_token
          , 'sourceIdentifier'
          , g_vc_source_identifier
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourceColumnList'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetColumnList'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlColumnList'
          , l_list_utl_col
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'utlValueList'
          , l_list_utl_val
         );
         --
         --
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'incrementColumn'
          , g_vc_increment_column
         );
         p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || '_'
                  || g_l_distr_code (i)
                  || ')'
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
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
         l_sql_prc := p#frm#ddls.c_template_prc_body;
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'prcParameters'
          , c_token_prc_param
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'varList'
          , CASE
               WHEN g_vc_increment_column IS NOT NULL THEN
                     'l_t_increment_bound '
                  || g_vc_increment_coldef
                  || ';'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'prcInitialize'
          , c_token_prc_initialize
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'prcFinalize'
          , c_token_prc_finalize
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'exceptionHandling'
          , c_token_prc_exception
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'prcBody'
          , l_sql_prc_token
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'sourceCode'
          , g_vc_source_code
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'objectName'
          , g_vc_object_name
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'sourceTable'
          , g_vc_table_name_source
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'prcName'
          ,    p#frm#stag_param.c_vc_procedure_load_stage_p
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

      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_load_stage_p;

   PROCEDURE prc_create_prc_load_diff (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name         t_object_name := 'prc_create_prc_load_diff';
      l_vc_message          t_string
                               :=    'Procedure load diff '
                                  || g_vc_package_main;
      l_sql_prc             CLOB;
      l_sql_prc_token       CLOB;
      l_sql_prc_buffer      CLOB;
      --
      l_n_iter_begin        NUMBER;
      l_n_iter_end          NUMBER;
      -- List of columns
      l_vc_col_list         t_string;
      l_vc_col_pk_hist      t_string;
      l_vc_clause_on        t_string;
      l_vc_upd_clause_set   t_string;
      l_vc_clause_history   t_string;
      l_vc_clause_update    t_string;
      l_vc_col_nvl2         t_string;
      -- Utl columns
      l_list_utl_col        t_string;
      l_list_utl_val        t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Begin'
      );
      --
      -- Set utl columns strings
      l_list_utl_col := c_token_utl_column_hist;
      -- Get list of pk columns of the History Table
      p#frm#trac.log_sub_trace (
         l_vc_prc_name
       , l_vc_message
       , 'Get list of pk columns of the History Table'
      );
      l_vc_col_pk_hist :=
         p#frm#dict.fct_get_column_list (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'PK'
          , 'LIST_SIMPLE'
          , p_vc_exclude_list   => p#frm#stag_param.c_vc_column_valid_to
         );
      p#frm#trac.log_sub_trace (
         l_vc_prc_name
       , l_vc_message
       , 'Got columns'
      );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      l_buffer_pkg_head :=
            l_buffer_pkg_head
         || CHR (10)
         || l_sql_prc;
      -- Hist incremental procedure head
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_diff_incr
      );
      p#frm#ddls.prc_set_text_param (
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
      p#frm#trac.log_sub_trace (
         l_vc_prc_name
       , l_vc_message
       , 'Get list of all columns in common within stage and hist tables'
      );
      l_vc_col_list :=
         p#frm#dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
         );
      p#frm#trac.log_sub_trace (
         l_vc_prc_name
       , l_vc_message
       , 'Got columns'
      );                                                                                                                                                                   -- In case the pk of stage 1 and History Tables is not the same, write a warning log

      IF g_vc_col_pk = l_vc_col_pk_hist
      OR (g_vc_col_pk IS NULL
      AND l_vc_col_pk_hist IS NULL) THEN
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          ,    'Source '
            || g_vc_source_code
            || ', Object '
            || g_vc_table_name_source
            || ' : Stage and hist table have the same Natural Keys'
         );
      ELSE
         p#frm#trac.log_sub_debug (
            l_vc_prc_name
          , l_vc_message
          ,    'Source '
            || g_vc_source_code
            || ', Object '
            || g_vc_table_name_source
            || ' : Stage and hist table have different Natural Keys'
         );
      END IF;

      -- analyze stage table
      l_sql_prc_token := c_token_analyze;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'STAN'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'partition'
       , NULL
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Analyse duplicates table
         l_sql_prc_token := c_token_analyze;
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'statisticsType'
          , 'DUAN'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'stgOwner'
          , g_vc_owner_stg
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_dupl
         );
         p#frm#ddls.prc_set_text_param (
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
         || c_token_diff_check;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'stgOwner'
       , g_vc_owner_stg
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'stageTableName'
       , g_vc_table_name_stage
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffTableName'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'histTableName'
       , g_vc_table_name_hist
      );

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NULL THEN
         -- If there is no natural key (tecnical PK) then use the alternate difference method
         l_vc_clause_on :=
            p#frm#dict.fct_get_column_subset (
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
            p#frm#dict.fct_get_column_list (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'PK'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
             , p_vc_exclude_list   => p#frm#stag_param.c_vc_column_valid_to
            );
         l_vc_col_nvl2 :=
            p#frm#dict.fct_get_column_subset (
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
         l_vc_clause_history :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'COMMON_NPK'
             , 'OR_DECODE'
             , 'trg'
             , 'src'
             , p_vc_exclude_list   => g_vc_col_update
            );
         l_vc_clause_update :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'COMMON_NPK'
             , 'OR_DECODE'
             , 'trg'
             , 'src'
             , p_vc_exclude_list   => g_vc_col_hist
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
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_diff_without_nk
            );
         ELSE
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'insertStatement'
             , p#frm#stmt.c_sql_insert_diff_with_nk
            );
         END IF;

         p#frm#dict.prc_set_text_param (
            l_sql_prc_token
          , 'enableParallelDML'
          , CASE
               WHEN l_vc_set_anonymized IS NOT NULL THEN
                  p#frm#stmt.c_token_enable_parallel_dml
               ELSE
                  p#frm#stmt.c_token_disable_parallel_dml
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'diffPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'sourcePartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
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
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffIdentifier'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceIdentifier'
       , g_vc_table_name_stage
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetIdentifier'
       , g_vc_table_name_hist
      );
      -- Set list of columns
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'nvl2ColumnList'
       , l_vc_col_nvl2
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetColumnList'
       , l_vc_col_list
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'utlColumnList'
       , l_list_utl_col
      );
      -- Set clauses
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'historyClause'
       , NVL (l_vc_clause_history, '1=0')
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'updateClause'
       , NVL (l_vc_clause_update, '1=0')
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'joinClause'
       , l_vc_clause_on
      );
      prc_set_utl_columns (l_sql_prc_buffer);
      -- Ad analyze token
      l_sql_prc_token := c_token_analyze;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'DFAN'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'partition'
       , NULL
      );
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || l_sql_prc_token;
      -- Put all other code parameters
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceCode'
       , g_vc_source_code
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'objectName'
       , g_vc_object_name
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceTable'
       , g_vc_table_name_source
      );
      --
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'joinType'
       , 'FULL'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_diff
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      --
      -- Load Hist without deletes
      --
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'joinType'
       , 'LEFT'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_diff_incr
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_load_diff;

   PROCEDURE prc_create_prc_load_hist (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name         t_object_name := 'prc_create_prc_load_hist';
      l_vc_message          t_string
                               :=    'Procedure load hist '
                                  || g_vc_package_main;
      l_sql_prc             CLOB;
      l_sql_prc_token       CLOB;
      l_sql_prc_buffer      CLOB;
      --
      l_n_iter_begin        NUMBER;
      l_n_iter_end          NUMBER;
      -- List of columns
      l_vc_col_simple       t_string;
      l_vc_clause_on        t_string;
      l_vc_upd_clause_set   t_string;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
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
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_hist
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage
          , g_vc_owner_stg
          , g_vc_table_name_hist
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
         );

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NULL THEN
         -- If there is no natural key (tecnical PK) then use the alternate difference method
         l_vc_clause_on :=
            p#frm#dict.fct_get_column_subset (
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
            p#frm#dict.fct_get_column_list (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_hist
             , 'PK'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
             , p_vc_exclude_list   => p#frm#stag_param.c_vc_column_valid_to
            );
         l_vc_upd_clause_set :=
            p#frm#dict.fct_get_column_subset (
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
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'closeStatement'
          , p#frm#stmt.c_sql_reconcile_close
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'updateStatement'
          , p#frm#stmt.c_sql_reconcile_update
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'insertStatement'
          , p#frm#stmt.c_sql_reconcile_insert
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'enableParallelDML'
          , CASE
               WHEN l_vc_set_anonymized IS NOT NULL THEN
                  p#frm#stmt.c_token_enable_parallel_dml
               ELSE
                  p#frm#stmt.c_token_disable_parallel_dml
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'diffPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'diffPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'targetPartition'
          , CASE
               WHEN g_vc_partition_expr IS NOT NULL THEN
                     'PARTITION ('
                  || p#frm#stag_param.c_vc_prefix_partition
                  || TO_CHAR (i)
                  || ')'
            END
         );
         p#frm#ddls.prc_set_text_param (
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
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffIdentifier'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetIdentifier'
       , g_vc_table_name_hist
      );
      -- Set list of columns
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'diffColumnList'
       ,    l_vc_col_simple
         || CHR (10)
         || l_vc_ins_anonymized
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'targetColumnList'
       ,    l_vc_col_simple
         || CHR (10)
         || l_vc_col_anonymized
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'utlColumnList'
       , c_token_utl_column_hist
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'utlValueList'
       , c_token_utl_colval_hist
      );
      -- Set clauses
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'joinClause'
       , l_vc_clause_on
      );
      -- Set the matched clause of the merge statement. This exists only if there are non-NK columns to set
      p#frm#ddls.prc_set_text_param (
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
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'statisticsType'
       , 'HSAN'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'stgOwner'
       , g_vc_owner_stg
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_hist
      );
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || l_sql_prc_token;                                                                                                                                                                                                   -- Put all other code parameters
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'sourceCode'
       , g_vc_source_code
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'objectName'
       , g_vc_object_name
      );
      --
      -- Load Hist with table comparison
      --
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_token_prc_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_token_prc_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_token_prc_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_load_hist
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_load_hist;

   PROCEDURE prc_create_prc_wrapper (
      p_b_tc_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name      t_object_name := 'prc_create_prc_wrapper';
      l_vc_message       t_string
                            :=    'Procedure wrapper '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Begin'
      );
      --
      -- HEAD for FULL load
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_wrapper
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          , NULL
         );
      ELSE
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          ,    p#frm#stag_param.c_vc_procedure_load_stage
            || ';'
         );
      END IF;

      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadDiff'
       ,    p#frm#stag_param.c_vc_procedure_load_diff
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadHist'
       ,    p#frm#stag_param.c_vc_procedure_load_hist
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncStage'
       ,    p#frm#stag_param.c_vc_procedure_trunc_stage
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncDiff'
       ,    p#frm#stag_param.c_vc_procedure_trunc_diff
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_hist
      );
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_wrapper
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      --
      -- HEAD for INCREMENTAL load
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_wrapper_incr
      );
      p#frm#ddls.prc_set_text_param (
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
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          , NULL
         );
      ELSE
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcLoadStage'
          ,    p#frm#stag_param.c_vc_procedure_load_stage
            || ';'
         );
      END IF;

      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadDiff'
       ,    p#frm#stag_param.c_vc_procedure_load_diff
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcLoadHist'
       ,    p#frm#stag_param.c_vc_procedure_load_diff_incr
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncStage'
       ,    p#frm#stag_param.c_vc_procedure_trunc_stage
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcTruncDiff'
       ,    p#frm#stag_param.c_vc_procedure_trunc_diff
         || ';'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_hist
      );
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_token_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , p#frm#stag_param.c_vc_procedure_wrapper_incr
      );
      l_buffer_pkg_body :=
            l_buffer_pkg_body
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_prc_wrapper;

   PROCEDURE prc_compile_package_main (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_prc_name   t_object_name := 'prc_compile_package_main';
      l_vc_message    t_string
                         :=    'Package compile '
                            || g_vc_package_main;
      l_sql_create    CLOB;
   BEGIN
      -- Package head
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Package head: Begin'
      );
      l_sql_create := p#frm#ddls.c_template_pkg_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'pkgName'
       , g_vc_package_main
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'varList'
       , ''
      );
      p#frm#ddls.prc_set_text_param (
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
      p#frm#ddls.prc_create_object (
         'PACKAGE'
       , g_vc_package_main
       , l_sql_create
       , FALSE
       , p_b_raise_flag
      );
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Package head: End'
      );
      -- Package body
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Package body: Begin'
      );
      l_sql_create := p#frm#ddls.c_template_pkg_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'varList'
       , ''
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'prcList'
       , l_buffer_pkg_body
      );
      p#frm#ddls.prc_set_text_param (
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
      p#frm#ddls.prc_create_object (
         'PACKAGE BODY'
       , g_vc_package_main
       , l_sql_create
       , FALSE
       , p_b_raise_flag
      );
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Package body: End'
      );
   END prc_compile_package_main;

   PROCEDURE prc_create_package_main (
      p_b_hist_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag        BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_prc_name   t_object_name := 'prc_create_package_main';
      l_vc_message    t_string
                         :=    'Package create '
                            || g_vc_package_main;
      l_sql_create    CLOB;
   BEGIN
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'Begin'
      );
      l_buffer_pkg_head := '';
      l_buffer_pkg_body := '';

      IF NOT p_b_hist_only_flag THEN
         -- Get list of columns for the stage 1 and init procedures
         l_vc_col_src :=
            p#frm#dict.fct_get_column_list (
               g_vc_dblink
             , g_vc_owner_src
             , g_vc_table_name_source
             , 'ALL'
             , 'LIST_SIMPLE'
            );
         l_vc_col_dupl :=
            p#frm#dict.fct_get_column_subset (
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
      -- Hist load
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
      p#frm#trac.log_sub_debug (
         l_vc_prc_name
       , l_vc_message
       , 'End'
      );
   END prc_create_package_main;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: stag_ddl-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_ddl/stag_ddl-impl.sql $';
END p#frm#stag_ddl;