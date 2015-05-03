CREATE OR REPLACE PACKAGE BODY p#frm#hist_ddl
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: hist_ddl-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/hist_ddl/hist_ddl-impl.sql $
   */
   /**
   * Templates for standard code tokens
   **/
   --
   c_sql_utl_columns            VARCHAR2 (100) := '#columnTimestamp#, #columnDmlOperation#';
   --
   c_sql_partition_diff         CLOB
                                   :=    'PARTITION BY LIST ('
                                      || p#frm#stag_param.c_vc_column_dml_op
                                      || ')
    (  
	  PARTITION PI VALUES (''I'') NOLOGGING NOCOMPRESS
    , PARTITION PU VALUES (''U'') NOLOGGING NOCOMPRESS
    , PARTITION PD VALUES (''D'') NOLOGGING NOCOMPRESS
    , PARTITION PR VALUES (''R'') NOLOGGING NOCOMPRESS
	)';
   c_sql_subpartition_diff      CLOB
                                   :=    'PARTITION BY LIST (#columnPartition#)
    SUBPARTITION BY LIST ('
                                      || p#frm#stag_param.c_vc_column_dml_op
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
   c_sql_partition              CLOB := 'PARTITION BY LIST (#columnPartition#)
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
   -- Create table template
   c_sql_create_table           CLOB := 'CREATE TABLE #tableName# (#listColUtl##listColumns#) #storageClause#';
   -- Template to add a primary key
   c_sql_create_pk              CLOB := 'ALTER TABLE #tableName# ADD (CONSTRAINT #pkName# PRIMARY KEY (#listColPk#) USING INDEX #storageClause#)';
   -- Template to add a primary key
   c_sql_create_notnull         CLOB := 'ALTER TABLE #tableName# MODIFY (#columnName# NOT NULL)';
   -- Enable/disable parallel execution
   c_sql_enable_parallel_dml    CLOB := 'EXECUTE IMMEDIATE ''ALTER SESSION ENABLE PARALLEL DML'';';
   c_sql_disable_parallel_dml   CLOB := 'EXECUTE IMMEDIATE ''ALTER SESSION DISABLE PARALLEL DML'';';
   -- Template to initialize run time statistics in a procedure
   -- Set the step number and the workflow
   c_sql_initialize             CLOB := '';
   -- Template to finalize run time statistics in a procedure
   -- Set the final step number and finalize job statistics
   c_sql_finalize               CLOB := '';
   -- Exception handler
   c_sql_exception              CLOB := 'p#frm#stag_stat.prc_stat_end(l_n_stat_id, 0, 1);';
   -- Standard parameters for a generated procedure
   c_sql_prc_param              CLOB := 'p_n_stream NUMBER DEFAULT NULL';
   -- Code body for the wrapper procedure
   c_sql_stag_wrapper           CLOB := '
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''Staging Begin'', ''Start extracting from #tableName#'');

		#prcStage1#

		#prcStage2#

		prc_trunc_stage1;

		prc_trunc_diff;

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''Staging End'', ''Stage completed for #tableName#'');';
   -- Check token of the init procedure
   c_sql_stg2_empty             CLOB := '
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
   -- Truncate token of the staging 1 procedure
   c_sql_pkg_table_trunc        CLOB := 'EXECUTE IMMEDIATE ''TRUNCATE TABLE #tableName# DROP STORAGE'';
		  p#frm#trac.log_sub_debug (l_vc_prc_name, ''TRUNCATE'', ''Table #tableName# truncated'');';
   -- Truncate token of the staging 1 procedure
   c_sql_pkg_part_trunc         CLOB := 'EXECUTE IMMEDIATE ''ALTER TABLE #tableName# TRUNCATE #tablePartition#'';
		  p#frm#trac.log_sub_debug (l_vc_prc_name, ''TRUNCATE'', ''Table #tableName# #tablePartition# truncated'');';
   -- Insert token of the staging 1 procedure
   c_sql_stg1_body_incr         CLOB := '
   
          p#frm#trac.log_sub_debug (l_vc_prc_name, ''INCR BOUND'', ''#tableName# #distracode# : get last #incrementColumn#'');
   
        SELECT MAX(#incrementColumn#)
          INTO l_t_increment_bound
          FROM #tableNameStage2# #tablePartition#;
          
          p#frm#trac.log_sub_debug (l_vc_prc_name, ''INCR BOUND'', ''#tableName# #distracode# : last #incrementColumn# = '' || l_t_increment_bound);
        
        ';
   -- Insert token of the staging 1 procedure
   c_sql_stg1_body_insert       CLOB := 'l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 1, #partition#, ''INS'');

        #computeIncrementBound#

		INSERT /*+APPEND*/ INTO #tableName# #tablePartition#
			(#backwardCompColumns##listColUtl##distracolumn##listColAllTrg#)
			SELECT #backwardCompValues##listExpUtl##distrValue##listColAllSrc#
			  FROM #owner##sourceTable##dblink#
                   #filterClause#;

		l_n_result := SQL%ROWCOUNT;

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

      COMMIT;

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''INSERT END'', ''#tableName# #distracode# : '' || l_n_result || '' rows inserted'');
		';
   -- Insert-deduplicate token of the staging 1 procedure
   c_sql_stg1_body_dedupl       CLOB := '
		l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 1, #partition#, ''INS'');

        #computeIncrementBound#

		INSERT /*+APPEND*/
		  WHEN row_rank = 1 AND
				 #notNullClause#
		  THEN
				INTO #tableName# #tablePartition#
					(#backwardCompColumns##listColUtl##distracolumn##listColAllTrg#)
			 VALUES
					(#backwardCompValues##listExpUtl##distrValue##listColAllSrc#)
		  ELSE
				INTO #tableNameDupl# #tablePartition#
					(#distracolumn##listColDupl#)
			 VALUES
					(#distrValue##listColDupl#)
		 SELECT #listColDupl#
				, ROW_NUMBER () over (PARTITION BY #listColPk# #deduplRankClause#) AS row_rank
			FROM #owner##sourceTable##dblink#
                 #filterClause#;

		l_n_result := SQL%ROWCOUNT;

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

      COMMIT;

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''INSERT END'', ''#tableName# #distracode# : '' || l_n_result || '' rows inserted'');
		';
   -- Statistics token of the staging 1 procedure
   c_sql_stg1_body_stats_stg1   CLOB := '
        l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 1, NULL, ''ANL'');
		
		DBMS_STATS.UNLOCK_TABLE_STATS (''#stgOwner#'', ''#tableName#'') ;
		DBMS_STATS.GATHER_TABLE_STATS (''#stgOwner#'', ''#tableName#'', NULL, 1);
		p#frm#stag_stat.prc_size_store(''#sourceCode#'', ''#sourceTable#'', ''#tableName#'');

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, 0);

		  p#frm#trac.log_sub_debug (l_vc_prc_name, ''STAT END'', ''#tableName# : Statistics gathered'');
		';
   -- Statistics token of the staging 1 procedure
   c_sql_stg1_body_stats_dupl   CLOB := '
		DBMS_STATS.UNLOCK_TABLE_STATS (''#stgOwner#'', ''#tableName#'') ;
		DBMS_STATS.GATHER_TABLE_STATS (''#stgOwner#'', ''#tableName#'', NULL, 1);
		p#frm#stag_stat.prc_size_store(''#sourceCode#'', ''#sourceTable#'', ''#tableName#'');

		  p#frm#trac.log_sub_debug (l_vc_prc_name, ''STAT END'', ''#tableName# : Statistics gathered'');
		';
   -- Check token of the staging 2 procedure
   c_sql_stg2_check             CLOB := '
        l_b_ok := p#frm#dict.fct_check_pk (
			NULL, ''#stgOwner#'', ''#tableNameStage1#'', ''#stgOwner#'', ''#tableNameStage2#''
		);
		IF l_b_ok THEN
			  p#frm#trac.log_sub_debug (l_vc_prc_name, ''CHECK NK'', ''#tableNameStage1# and #tableNameStage2# have the same NK'');
		ELSE
			  p#frm#trac.log_sub_warn (l_vc_prc_name, ''CHECK NK'', ''#tableNameStage1# and #tableNameStage2# have not the same NK'');		
		END IF;
        
        SELECT COUNT(*) INTO l_n_result FROM #tableNameStage1#;
        
        IF l_n_result = 0 THEN
            p#frm#trac.log_sub_error (l_vc_prc_name, ''CHECK'', ''Table #tableNameStage1# is empty'');
            raise_application_error (-20000, ''Stage1 table is empty.'');        
        END IF;
        
        EXECUTE IMMEDIATE ''ALTER SESSION ENABLE PARALLEL DML'';
		
		-- Truncate Diff table
		 p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF TRUNCATE'', ''Truncate #tableNameDiff#'');		
		EXECUTE IMMEDIATE ''TRUNCATE TABLE #tableNameDiff# DROP STORAGE'';
		 p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF TRUNCATE'', ''#tableNameDiff# truncated'');
		';
   -- Diff token of the staging 2 procedure - nk present
   c_sql_stg2_body_diff_nk      CLOB := '
		 p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF BEGIN'', ''Insert into #tableNameDiff#'');

		l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 2, #partition#, ''FDI'');
		
		INSERT
		INTO #tableNameDiff# #tablePartitionStage2# (
			#listColAll#
		  , #listColUtl#)
		SELECT
			#listColAll#
		  , #listColUtl#
		FROM (SELECT
				 #listColAllNVL2#
				, CASE
						WHEN src.rowid IS NOT NULL
						AND trg.rowid  IS NULL
						THEN ''I'' -- new row in src
						WHEN src.rowid       IS NULL
						AND trg.rowid        IS NOT NULL
						AND trg.#columnDmlOperation# <> ''D''
						THEN ''D'' -- row was deleted in src
						WHEN src.rowid      IS NOT NULL
						AND trg.rowid       IS NOT NULL
						AND trg.#columnDmlOperation# = ''D''
						THEN ''R'' -- row was deleted and now reappeared
						WHEN src.rowid IS NOT NULL
						AND trg.rowid  IS NOT NULL
						AND (#updateClause#)
						THEN ''U''
						ELSE NULL -- nothing to be done
					END AS #columnDmlOperation#
                  , trg.#columnTimestamp#
				FROM #tableNameStage1# #tablePartitionStage1# src
				#joinType# OUTER JOIN #tableNameStage2# #tablePartitionStage2# trg
				ON	#listOnClause#)
		WHERE
			#columnDmlOperation# IS NOT NULL;

		l_n_result := SQL%ROWCOUNT;

      COMMIT;

	  p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);
		
      p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF INSERTED'', ''#tableNameDiff# : '' || l_n_result || '' rows inserted'');
		
      DBMS_STATS.UNLOCK_TABLE_STATS (''#stgOwner#'', ''#tableNameDiff#'') ;
	  DBMS_STATS.GATHER_TABLE_STATS (''#stgOwner#'', ''#tableNameDiff#'', NULL, 1);
	  p#frm#stag_stat.prc_size_store(''#sourceCode#'', ''#sourceTable#'', ''#tableNameDiff#'');
		
      p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF ANALYZED'', ''#tableNameDiff# analyzed'');
';
   -- Diff token of the staging 2 procedure - nk non-present
   c_sql_stg2_body_diff_nonk    CLOB := '
	    p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF BEGIN'', ''Insert into #tableNameDiff#'');

		l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 2, #partition#, ''FDI'');
		
		INSERT
		INTO #tableNameDiff# #tablePartitionStage2# (
			#listColAll#
		  , #listColUtl#)
		SELECT
			#listColAll#
		  , #listColUtl#
		FROM (SELECT #listColAll#
             , #columnTimestamp#
             , CASE
                  WHEN cnt_in_src > 0
                  AND cnt_in_dst = 0
                     THEN ''I''                                                                                                                                                          -- new row in src
                  WHEN cnt_in_src > 0
                  AND cnt_in_dst > 0
                  AND #columnDmlOperation#_dst = ''D''
                     THEN ''R''
                  WHEN cnt_in_src = 0
                  AND cnt_in_dst > 0
                  AND #columnDmlOperation#_dst <> ''D''
                     THEN ''D''
                  ELSE NULL
               END AS #columnDmlOperation#
          FROM (SELECT   #listColAll#
                       , MAX (#columnTimestamp#) AS #columnTimestamp#
                       , MAX (#columnDmlOperation#_dst) AS #columnDmlOperation#_dst
                       , COUNT (rowid_src) AS cnt_in_src
                       , COUNT (rowid_dst) AS cnt_in_dst
                    FROM (SELECT #listColAll#
                               , NULL AS #columnTimestamp#
                               , NULL AS #columnDmlOperation#_dst
                               , ROWID AS rowid_src
                               , NULL AS rowid_dst
                            FROM #tableNameStage1# #tablePartitionStage1#
                          UNION ALL
                          SELECT #listColAll#
                               , #columnTimestamp#
                               , #columnDmlOperation# AS #columnDmlOperation#_dst
                               , NULL AS rowid_src
                               , ROWID AS rowid_dst
                            FROM #tableNameStage2# #tablePartitionStage2#)
                GROUP BY #listColAll#))
		WHERE
			#columnDmlOperation# #operationClause#;

		l_n_result := SQL%ROWCOUNT;

      COMMIT;

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);
		
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF INSERTED'', ''#tableNameDiff# : '' || l_n_result || '' rows inserted'');
		
		DBMS_STATS.UNLOCK_TABLE_STATS (''#stgOwner#'', ''#tableNameDiff#'') ;
		DBMS_STATS.GATHER_TABLE_STATS (''#stgOwner#'', ''#tableNameDiff#'', NULL, 1);
		p#frm#stag_stat.prc_size_store(''#sourceCode#'', ''#sourceTable#'', ''#tableNameDiff#'');
		
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF ANALYZED'', ''#tableNameDiff# analyzed'');
';
   -- Merge token of the staging 2 procedure - 1 single statement
   c_sql_stg2_body_stg2_1dml    CLOB := '
        EXECUTE IMMEDIATE ''ALTER SESSION ENABLE PARALLEL DML'';
		
		-- Update Stage2 table
		
		 p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF UPDATE'', ''Update #tableNameStage2#'');
		l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 2, #partition#, ''FUP'');

      MERGE /*+APPEND*/
		 INTO #tableNameStage2# trg
      USING
			(SELECT DECODE (#columnDmlOperation#, ''R'', ''U'', #columnDmlOperation#) AS #columnDmlOperation#
					, #listColAll#
				FROM #tableNameDiff# #tablePartitionStage2#) src
				  ON (#listOnClause#)
		WHEN MATCHED THEN
			 UPDATE
				 SET #matchedClause#
					  trg.#columnDmlOperation# = src.#columnDmlOperation#
					, trg.#columnTimestamp# = SYSDATE
        WHEN NOT MATCHED THEN
             INSERT (
				#listColTarget#
				#backwardCompColumns#
			  , #listColUtl#
             )
             VALUES (
                #listColSource#
				#backwardCompValues#
				#listValUtl#
             );

      l_n_result := SQL%ROWCOUNT;

      p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

	  COMMIT;

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF END'', ''#tableNameStage2# : '' || l_n_result || '' rows inserted'');';
   -- Merge token of the staging 2 procedure - 2 separate statement
   c_sql_stg2_body_stg2_2dml    CLOB := '
        #enableParallelDML#
		
		-- Update Stage2 table
		
		 p#frm#trac.log_sub_debug (l_vc_prc_name, ''STG2 UPDATE'', ''Update #tableNameStage2#'');
		l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 2, #partition#, ''FUP'');

      MERGE /*+APPEND*/
		 INTO #tableNameStage2# trg
      USING
			(SELECT DECODE (#columnDmlOperation#, ''R'', ''U'', #columnDmlOperation#) AS #columnDmlOperation#
					, #listColAll#
				FROM #tableNameDiff# #tablePartitionStage2#
			  WHERE #columnDmlOperation# IN (''U'', ''R'', ''D'')) src
				  ON (#listOnClause#)
		WHEN MATCHED THEN
			 UPDATE
				 SET #matchedClause#
					  trg.#columnDmlOperation# = src.#columnDmlOperation#
					, trg.#columnTimestamp# = SYSDATE;

		l_n_result := SQL%ROWCOUNT;

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

      COMMIT;
		
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''STG2 UPDATED'', ''#tableNameStage2# : '' || l_n_result || '' rows updated'');
		
		-- Insert into Stage2 table
		
        p#frm#trac.log_sub_debug (l_vc_prc_name, ''STG2 INSERT'', ''#tableNameStage2# : Insert'');

	  l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 2, #partition#, ''FIN'');

	  INSERT /*+APPEND*/ INTO #tableNameStage2# #tablePartitionStage2# (
											#listColTarget#
											#backwardCompColumns#
										  , #listColUtl#)
							  SELECT #listColSource#
									 #backwardCompValues#
									 #listValUtl#
								FROM #tableNameDiff# #tablePartitionStage2#
							   WHERE #columnDmlOperation# = ''I'';

      l_n_result := SQL%ROWCOUNT;

      p#frm#stag_stat.prc_stat_end(l_n_stat_id, l_n_result);

	  COMMIT;

        p#frm#trac.log_sub_debug (l_vc_prc_name, ''DIFF END'', ''#tableNameStage2# : '' || l_n_result || '' rows inserted'');';
   c_sql_stg2_body_stats        CLOB := '
        l_n_stat_id := p#frm#stag_stat.prc_stat_begin(''#sourceCode#'', ''#objectName#'', 2, NULL, ''ANL'');

		DBMS_STATS.UNLOCK_TABLE_STATS (''#stgOwner#'', ''#tableNameStage2#'') ;
		DBMS_STATS.GATHER_TABLE_STATS (''#stgOwner#'', ''#tableNameStage2#'', NULL, 1);
		p#frm#stag_stat.prc_size_store(''#sourceCode#'', ''#sourceTable#'', ''#tableNameStage2#'');

		p#frm#stag_stat.prc_stat_end(l_n_stat_id, 0);

		  p#frm#trac.log_sub_debug (l_vc_prc_name, ''STAT END'', ''#tableNameStage2# : Statistics gathered'');
		';
   -- Buffers
   l_sql_pkg_head_buffer        CLOB;
   l_sql_pkg_body_buffer        CLOB;
   l_vc_col_src                 p#frm#type.vc_max_plsql;
   l_vc_col_dupl                p#frm#type.vc_max_plsql;
   l_vc_col_pk_notnull          p#frm#type.vc_max_plsql;
   -- Anonymization
   l_vc_def_anonymized          p#frm#type.vc_max_plsql;
   l_vc_col_anonymized          p#frm#type.vc_max_plsql;
   l_vc_set_anonymized          p#frm#type.vc_max_plsql;
   l_vc_ins_anonymized          p#frm#type.vc_max_plsql;
   l_vc_fct_anonymized          p#frm#type.vc_max_plsql;
   l_vc_ini_anonymized          p#frm#type.vc_max_plsql;
   l_vc_viw_anonymized          p#frm#type.vc_max_plsql;

   FUNCTION fct_get_identifier (
      p_vc_dblink         VARCHAR2
    , p_vc_schema_name    VARCHAR2
    , p_vc_object_name    VARCHAR2
   )
      RETURN VARCHAR2
   IS
   BEGIN
      RETURN NULL;
   END;

   PROCEDURE prc_set_tech_columns (p_vc_code_string IN OUT CLOB)
   IS
      l_vc_prc_name   p#frm#type.vc_max_plsql := 'PRC_SET_SRC_PARAM';
   BEGIN
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'columnTimestamp'
       , p#frm#stag_param.c_vc_column_timestamp
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'columnDmlOperation'
       , p#frm#stag_param.c_vc_column_dml_op
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'columnPartition'
       , p#frm#stag_param.c_vc_column_partition
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'columnActiveVersion'
       , p#frm#stag_param.c_vc_column_active_version
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'columnValidFrom'
       , p#frm#stag_param.c_vc_column_valid_from
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'columnValidTo'
       , p#frm#stag_param.c_vc_column_valid_to
      );
      p#frm#ddls.prc_set_text_param (
         p_vc_code_string
       , 'columnSourceDistribution'
       , p#frm#stag_param.c_vc_column_source_distr
      );
   END prc_set_tech_columns;

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
                         AND exi.table_name = UPPER (g_vc_table_name_stage2)
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
      l_vc_message         VARCHAR2 (32000)
                              :=    'Table stage 1 '
                                 || g_vc_table_name_stage1;
      l_sql_create         CLOB;
      l_sql_list_col_utl   VARCHAR2 (32000);
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 1 Table: Begin'
      );
      l_sql_list_col_utl :=
         CASE
            WHEN g_l_distr_code.COUNT > 1 THEN
               '#columnSourceDistribution# VARCHAR(12),'
            WHEN g_vc_partition_clause IS NOT NULL THEN
               '#columnPartition# NUMBER(1),'
         END;
      -- Build create table statement
      l_sql_create := c_sql_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_stage1
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_sql_list_col_utl
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
               WHEN g_vc_tablespace_stg1_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stg1_data
            END
      );

      -- Partitions
      IF g_l_distr_code.COUNT > 1 THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ' PARTITION BY LIST (#columnSourceDistribution#) (';

         FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
            IF i > 1 THEN
               l_sql_create :=
                     l_sql_create
                  || ',';
            END IF;

            l_sql_create :=
                  l_sql_create
               || CHR (10)
               || ' PARTITION P_'
               || g_l_distr_code (i)
               || ' VALUES ('''
               || g_l_distr_code (i)
               || ''') NOLOGGING COMPRESS';
         END LOOP;

         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ')';
      ELSIF g_vc_partition_clause IS NOT NULL THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || c_sql_partition;
      END IF;

      prc_set_tech_columns (l_sql_create);
      prc_store_ddl (
         'TABLE'
       , g_vc_table_name_stage1
       , l_sql_create
      );

      BEGIN
         p#frm#trac.log_info (
            l_vc_message
          , 'Creating table...'
         );
         p#frm#ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_stage1
          , l_sql_create
          , p_b_drop_flag
          , TRUE
         );
         p#frm#trac.log_info (
            l_vc_message
          , 'Table created'
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_error (
               'Stage 1 Table: Warning'
             , SQLERRM
            );
            RAISE;
      END;

      BEGIN
         p#frm#trac.log_info (
            l_vc_message
          , 'Setting compression option...'
         );

         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_stage1
            || ' COMPRESS FOR QUERY LOW';

         p#frm#trac.log_info (
            l_vc_message
          , 'Compression option set'
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_info (
               SQLERRM
             , 'FOR QUERY LOW option not available'
            );
      END;

      -- Build constraint statement
      /*l_sql_create          := c_sql_create_pk;
      p#frm#ddls.prc_set_text_param (l_sql_create
                                    , 'tableName'
                                    , g_vc_table_name_stage1
                                     );
      p#frm#ddls.prc_set_text_param (l_sql_create
                                    , 'pkName'
                                    , g_vc_nk_name_stage1
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
                                         WHEN g_vc_tablespace_stg1_indx IS NOT NULL
                                            THEN ' TABLESPACE ' || g_vc_tablespace_stg1_indx
                                      END
                                     );
      prc_set_tech_columns (l_sql_create);
      prc_store_ddl ('CONSTRAINT'
                   , g_vc_nk_name_stage1
                   , l_sql_create
                    );

      BEGIN
           p#frm#trac.log_info (l_vc_message, 'Creating NK...');
         p#frm#ddls.prc_create_object ('CONSTRAINT'
                                      , g_vc_nk_name_stage1
                                      , l_sql_create
                                      , p_b_drop_flag
                                      , TRUE
                                       );
           p#frm#trac.log_info (l_vc_message, 'NK created');
      EXCEPTION
         WHEN OTHERS
         THEN
              p#frm#trac.log_info (SQLERRM
                           , 'NK not created'
                           , param.gc_log_warn
                            );
            RAISE;
      END;*/
      IF g_n_parallel_degree > 1 THEN
         p#frm#trac.log_info (
            l_vc_message
          , 'Setting parallel option...'
         );

         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_stage1
            || ' PARALLEL '
            || g_n_parallel_degree;

         p#frm#trac.log_info (
            l_vc_message
          , 'Parallel option set...'
         );
      END IF;

      -- Comments from source system
      p#frm#trac.log_info (
         l_vc_message
       , 'Setting comments...'
      );

      EXECUTE IMMEDIATE
            'COMMENT ON TABLE '
         || g_vc_table_name_stage1
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
            || g_vc_table_name_stage1
            || '.'
            || r_comm.stag_column_name
            || ' IS '''
            || r_comm.stag_column_comment
            || '''';
      END LOOP;

      p#frm#trac.log_info (
         l_vc_message
       , 'Comments set...'
      );
      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 1 Table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_info (
            SQLERRM
          , 'Stage 1 Table: Error'
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
      l_vc_message         VARCHAR2 (32000)
                              :=    'Table duplicates '
                                 || g_vc_table_name_dupl;
      l_sql_create         CLOB;
      l_sql_list_col_utl   VARCHAR2 (32000);
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Duplicates Table: Begin'
      );
      l_sql_list_col_utl :=
         CASE
            WHEN g_l_distr_code.COUNT > 1 THEN
               '#columnSourceDistribution# VARCHAR(12),'
            WHEN g_vc_partition_clause IS NOT NULL THEN
               '#columnPartition# NUMBER(1),'
         END;
      -- Build create table statement
      l_sql_create := c_sql_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_dupl
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_sql_list_col_utl
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
               WHEN g_vc_tablespace_stg1_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stg1_data
            END
      );

      -- Stage1 partitions
      IF g_l_distr_code.COUNT > 1 THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ' PARTITION BY LIST (#columnSourceDistribution#) (';

         FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
            IF i > 1 THEN
               l_sql_create :=
                     l_sql_create
                  || ',';
            END IF;

            l_sql_create :=
                  l_sql_create
               || CHR (10)
               || 'PARTITION P_'
               || g_l_distr_code (i)
               || ' VALUES ('''
               || g_l_distr_code (i)
               || ''') NOLOGGING COMPRESS';
         END LOOP;

         l_sql_create :=
               l_sql_create
            || CHR (10)
            || ')';
      ELSIF g_vc_partition_clause IS NOT NULL THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || c_sql_partition;
      END IF;

      prc_set_tech_columns (l_sql_create);
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
            p#frm#trac.log_error (
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
         p#frm#ddls.prc_execute (l_sql_create);
      END IF;

      p#frm#trac.log_info (
         l_vc_message
       , 'Duplicates Table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_info (
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
      l_vc_message             VARCHAR2 (32000)
                                  :=    'Table difference '
                                     || g_vc_table_name_diff;
      l_sql_create             CLOB;
      l_sql_subpart_template   VARCHAR2 (32000);
      l_sql_list_col_utl       VARCHAR2 (32000);
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Difference table: Begin'
      );
      l_sql_list_col_utl :=
            '#columnTimestamp# DATE, 
			 #columnDmlOperation# VARCHAR2(2 CHAR),'
         || CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  '#columnSourceDistribution# VARCHAR(12),'
               WHEN g_vc_partition_clause IS NOT NULL THEN
                  '#columnPartition# NUMBER(1),'
            END;
      l_sql_create := c_sql_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_sql_list_col_utl
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
               WHEN g_vc_partition_clause IS NOT NULL THEN
                  c_sql_subpartition_diff
               ELSE
                  c_sql_partition_diff
            END
         || CASE
               WHEN g_vc_tablespace_stg1_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stg1_data
            END
      );
      prc_set_tech_columns (l_sql_create);
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
            p#frm#trac.log_error (
               'Difference Table: Warning'
             , SQLERRM
            );
            RAISE;
      END;

      l_sql_create := c_sql_create_pk;
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
               WHEN g_vc_tablespace_stg1_indx IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stg1_indx
            END
      );
      prc_set_tech_columns (l_sql_create);
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
            p#frm#trac.log_error (
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
         p#frm#ddls.prc_execute (l_sql_create);
      END IF;

      p#frm#trac.log_info (
         l_vc_message
       , 'Difference table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_info (
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
      l_vc_message         VARCHAR2 (32000)
                              :=    'Table stage 2 '
                                 || g_vc_table_name_stage2;
      l_sql_create         p#frm#type.vc_max_plsql;
      l_sql_list_col_utl   p#frm#type.vc_max_plsql;
      l_l_utl_columns      DBMS_SQL.varchar2s;
      l_sql_utl_columns    p#frm#type.vc_max_plsql := c_sql_utl_columns;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Diff Table: Begin'
      );
      prc_set_tech_columns (l_sql_utl_columns);
      -- Set anonymizad column lists
      l_vc_def_anonymized := '';
      l_vc_ini_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_coldefs;
      -- Generate table ddl
      l_sql_list_col_utl :=
            '#columnTimestamp# DATE, 
			#columnDmlOperation# VARCHAR2(2 CHAR),'
         || CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  '#columnSourceDistribution# VARCHAR(12),'
               WHEN g_vc_partition_clause IS NOT NULL THEN
                  '#columnPartition# NUMBER(1),'
            END;
      l_sql_create := c_sql_create_table;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_stage2
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColUtl'
       , l_sql_list_col_utl
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
               WHEN g_vc_tablespace_stg2_data IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stg2_data
            END
      );

      IF g_vc_partition_clause IS NOT NULL THEN
         l_sql_create :=
               l_sql_create
            || CHR (10)
            || c_sql_partition;
      END IF;

      -- Execute table ddl
      prc_set_tech_columns (l_sql_create);
      prc_store_ddl (
         'TABLE'
       , g_vc_table_name_stage2
       , l_sql_create
      );

      BEGIN
         -- Try to create table
         p#frm#ddls.prc_create_object (
            'TABLE'
          , g_vc_table_name_stage2
          , l_sql_create
          , FALSE
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_error (
               'Stage 2 Table Create: Warning'
             , SQLERRM
            );

            IF l_vc_def_anonymized IS NOT NULL THEN
               BEGIN
                  p#frm#trac.log_info (
                     'Add new anonymized columns'
                   , 'Stage 2 Table Add Anonymized'
                  );

                  -- Try to add newly anonymized columns
                  EXECUTE IMMEDIATE
                        'ALTER TABLE '
                     || g_vc_table_name_stage2
                     || ' ADD ('
                     || LTRIM (
                           l_vc_def_anonymized
                         , ','
                        )
                     || ')';
               EXCEPTION
                  WHEN OTHERS THEN
                     p#frm#trac.log_warn (
                        SQLERRM
                      , 'Stage 2 Table Add Anonymized: Warning'
                     );

                     IF p_b_raise_flag THEN
                        RAISE;
                     END IF;
               END;
            END IF;

            IF l_vc_ini_anonymized IS NOT NULL THEN
               BEGIN
                  p#frm#trac.log_info (
                     'Fill new anonymized columns'
                   , 'Stage 2 Table Upd Anonymized'
                  );

                  -- Try to fill newly added anonymized columns
                  EXECUTE IMMEDIATE
                        'UPDATE '
                     || g_vc_table_name_stage2
                     || ' SET '
                     || LTRIM (
                           l_vc_ini_anonymized
                         , ','
                        );

                  COMMIT;
               EXCEPTION
                  WHEN OTHERS THEN
                     p#frm#trac.log_warn (
                        SQLERRM
                      , 'Stage 2 Table Upd Anonymized: Warning'
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
            || g_vc_table_name_stage2
            || ' PARALLEL '
            || g_n_parallel_degree;
         p#frm#ddls.prc_execute (l_sql_create);
      END IF;

      IF g_vc_fb_archive IS NOT NULL
     AND g_n_fbda_flag = 1 THEN
         BEGIN
            EXECUTE IMMEDIATE
                  'ALTER TABLE '
               || g_vc_table_name_stage2
               || ' FLASHBACK ARCHIVE '
               || g_vc_fb_archive;
         EXCEPTION
            WHEN OTHERS THEN
               p#frm#trac.log_info (
                  SQLERRM
                , 'Stage 2 Table: FLASHBACK'
               );
         END;
      END IF;

      BEGIN
         EXECUTE IMMEDIATE
               'ALTER TABLE '
            || g_vc_table_name_stage2
            || ' COMPRESS FOR QUERY LOW';
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_info (
                  SQLERRM
               || ' - FOR QUERY LOW option not available'
             , 'Stage 2 Table: COMPRESS'
            );
      END;

      -- Generate NK ddl
      l_sql_create := c_sql_create_pk;
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'tableName'
       , g_vc_table_name_stage2
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'pkName'
       , g_vc_nk_name_stage2
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'listColPk'
       , g_vc_col_pk
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
                     , g_vc_table_name_stage2
                    ) THEN
                  'LOCAL'
            END
         || CASE
               WHEN g_vc_tablespace_stg2_indx IS NOT NULL THEN
                     ' TABLESPACE '
                  || g_vc_tablespace_stg2_indx
            END
      );
      -- Execute NK ddl
      prc_set_tech_columns (l_sql_create);
      prc_store_ddl (
         'CONSTRAINT'
       , g_vc_nk_name_stage2
       , l_sql_create
      );

      BEGIN
         p#frm#ddls.prc_create_object (
            'CONSTRAINT'
          , g_vc_nk_name_stage2
          , l_sql_create
          , FALSE
          , TRUE
         );
      EXCEPTION
         WHEN OTHERS THEN
            p#frm#trac.log_warn (
               SQLERRM
             , 'Stage 2 Natural Key: Warning'
            );

            IF p_b_raise_flag THEN
               RAISE;
            END IF;
      END;

      l_l_utl_columns :=
         p#frm#type.fct_string_to_list (
            l_sql_utl_columns
          , ','
         );

      FOR i IN l_l_utl_columns.FIRST .. l_l_utl_columns.LAST LOOP
         l_sql_create := c_sql_create_notnull;
         p#frm#ddls.prc_set_text_param (
            l_sql_create
          , 'tableName'
          , g_vc_table_name_stage2
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_create
          , 'columnName'
          , l_l_utl_columns (i)
         );
         -- Execute Check ddl
         prc_set_tech_columns (l_sql_create);
         prc_store_ddl (
            'CONSTRAINT'
          ,    SUBSTR (
                  g_vc_nk_name_stage2
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
                     g_vc_nk_name_stage2
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
                , 'Stage 2 Natural Key: Warning'
               );

               IF p_b_raise_flag THEN
                  RAISE;
               END IF;
         END;
      END LOOP;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_table_name_stage2
         || ' TO '
         || p#frm#stag_param.c_vc_list_grantee;

      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 2 Table: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_info (
            SQLERRM
          , 'Stage 2 Table: Error'
         );

         IF p_b_raise_flag THEN
            RAISE;
         END IF;
   END prc_create_hist_table;

   PROCEDURE prc_create_hist_view (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message   VARCHAR2 (32000)
                        :=    'View stage 2 '
                           || g_vc_view_name_stage2;
      l_sql_create   p#frm#type.vc_max_plsql;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 2 View: Begin'
      );
      l_vc_viw_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_viewcols;
      --
      l_sql_create :=
            'CREATE OR REPLACE FORCE VIEW '
         || g_vc_view_name_stage2
         || ' AS SELECT '
         || NVL (
               LTRIM (
                  l_vc_viw_anonymized
                , ','
               )
             , '*'
            )
         || ' FROM '
         || g_vc_table_name_stage2;
      prc_store_ddl (
         'VIEW'
       , g_vc_view_name_stage2
       , l_sql_create
      );

      EXECUTE IMMEDIATE l_sql_create;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_view_name_stage2
         || ' TO '
         || p#frm#stag_param.c_vc_list_grantee;

      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 2 View: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_info (
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
      l_vc_message   VARCHAR2 (32000)
                        :=    'Synonym stage 2 '
                           || g_vc_view_name_stage2;
      l_sql_create   p#frm#type.vc_max_plsql;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 2 Synonym: Begin'
      );
      l_vc_viw_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_viewcols;
      --
      l_sql_create :=
            'CREATE OR REPLACE SYNONYM '
         || g_vc_view_name_stage2
         || ' FOR '
         || g_vc_table_name_stage2;
      prc_store_ddl (
         'SYNONYM'
       , g_vc_view_name_stage2
       , l_sql_create
      );

      EXECUTE IMMEDIATE l_sql_create;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_view_name_stage2
         || ' TO '
         || p#frm#stag_param.c_vc_list_grantee;

      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 2 Synonym: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_info (
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
      l_vc_message   VARCHAR2 (32000)
                        :=    'View stage 2 '
                           || g_vc_view_name_stage2;
      l_sql_create   p#frm#type.vc_max_plsql;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 2 View: Begin'
      );
      l_vc_viw_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_viewcols;
      --
      l_sql_create :=
            'CREATE OR REPLACE FORCE VIEW '
         || g_vc_view_name_history
         || ' AS SELECT versions_starttime
     , versions_startscn
     , versions_endtime
     , versions_endscn
     , versions_xid
     , versions_operation
     '
         || l_vc_viw_anonymized
         || ' FROM '
         || g_vc_table_name_stage2
         || ' VERSIONS BETWEEN TIMESTAMP MINVALUE AND MAXVALUE';
      prc_store_ddl (
         'VIEW'
       , g_vc_view_name_history
       , l_sql_create
      );

      EXECUTE IMMEDIATE l_sql_create;

      EXECUTE IMMEDIATE
            'GRANT SELECT ON '
         || g_vc_view_name_history
         || ' TO '
         || p#frm#stag_param.c_vc_list_grantee;

      p#frm#trac.log_info (
         l_vc_message
       , 'Stage 2 View: End'
      );
   EXCEPTION
      WHEN OTHERS THEN
         p#frm#trac.log_info (
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
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure Trunc stage1 '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_trunc_stage1'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      l_sql_prc_token := c_sql_pkg_table_trunc;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage1
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         l_sql_prc_token := c_sql_pkg_table_trunc;
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
       , c_sql_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_sql_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_sql_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_sql_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_trunc_stage1'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_trunc_stage;

   PROCEDURE prc_create_prc_trunc_diff (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure Trunc diff '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_trunc_diff'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      l_sql_prc_buffer := c_sql_pkg_table_trunc;
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
       , c_sql_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_sql_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_sql_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_sql_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_trunc_diff'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_trunc_diff;

   PROCEDURE prc_create_prc_init (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message        VARCHAR2 (32000)
                             :=    'Procedure load init '
                                || g_vc_package_main;
      l_sql_prc           CLOB;
      l_sql_prc_token     CLOB;
      l_sql_prc_buffer    CLOB;
      -- List of columns
      l_vc_col_all        p#frm#type.vc_max_plsql;
      l_sql_utl_columns   p#frm#type.vc_max_plsql := c_sql_utl_columns;
   BEGIN
      l_vc_col_anonymized := '';
      l_vc_fct_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_columns;
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );
      prc_set_tech_columns (l_sql_utl_columns);
      -- Get lists of columns
      l_vc_col_all :=
         p#frm#dict.fct_get_column_subset (
            g_vc_dblink
          , g_vc_owner_src
          , g_vc_table_name_source
          , g_vc_owner_stg
          , g_vc_table_name_stage2
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
          , p_vc_exclude_list   => l_sql_utl_columns
         );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_init'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Check if stage 2 is empty
      l_sql_prc_token :=
            'EXECUTE IMMEDIATE ''ALTER SESSION ENABLE PARALLEL DML'';'
         || CHR (10)
         || c_sql_stg2_empty;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage2
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Truncate duplicates table
         l_sql_prc_token := c_sql_pkg_table_trunc;
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

      -- Fill stage 1 for each source db
      FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
         IF g_n_source_nk_flag = 0
        AND g_vc_col_pk_src IS NOT NULL THEN
            l_vc_col_pk_notnull :=
               p#frm#stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            l_sql_prc_token := c_sql_stg1_body_dedupl;
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'tableNameDupl'
             , g_vc_table_name_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
         ELSE
            l_sql_prc_token := c_sql_stg1_body_insert;
         END IF;

         -- Add optional increment retrieval statement
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'computeIncrementBound'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'dblink'
          , CASE
               WHEN g_l_dblink (i) IS NOT NULL THEN
                     '@'
                  || g_l_dblink (i)
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'owner'
          ,    g_l_owner_src (i)
            || '.'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_stage2
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tablePartition'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColAllTrg'
          ,    l_vc_col_all
            || CHR (10)
            || l_vc_col_anonymized
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColAllSrc'
          ,    l_vc_col_all
            || CHR (10)
            || l_vc_fct_anonymized
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColDupl'
          , l_vc_col_dupl
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColPk'
          , g_vc_col_pk_src
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColUtl'
          ,    c_sql_utl_columns
            || ', '
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listExpUtl'
          , ' SYSDATE, ''I'','
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'distracolumn'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  '#columnSourceDistribution#, '
               WHEN g_vc_partition_clause IS NOT NULL THEN
                  '#columnPartition#, '
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'distrValue'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     ''''
                  || g_l_distr_code (i)
                  || ''', '
               WHEN g_vc_partition_clause IS NOT NULL THEN
                     ' CASE WHEN TRIM( TRANSLATE ('
                  || g_vc_partition_clause
                  || ',''0123456789'',''          '')) IS NULL THEN TO_NUMBER('
                  || g_vc_partition_clause
                  || ') ELSE 0 END, '
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'distracode'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  g_l_distr_code (i)
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'filterClause'
          , CASE
               WHEN g_vc_filter_clause IS NOT NULL THEN
                     ' WHERE '
                  || g_vc_filter_clause
            END
         );
         l_sql_prc_buffer :=
               l_sql_prc_buffer
            || CHR (10)
            || l_sql_prc_token;
      END LOOP;

      l_sql_prc_token := c_sql_stg1_body_stats_stg1;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage2
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
         l_sql_prc_token := c_sql_stg1_body_stats_dupl;
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
       , c_sql_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_sql_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_sql_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_sql_exception
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
       , 'prc_load_init'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_init;

   PROCEDURE prc_create_prc_stage (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure load stage1 '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_stage1'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Truncate stage1 table
      l_sql_prc_token :=
            'EXECUTE IMMEDIATE ''ALTER SESSION ENABLE PARALLEL DML'';'
         || CHR (10)
         || c_sql_pkg_table_trunc;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage1
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk_src IS NOT NULL THEN
         -- Truncate duplicates table
         l_sql_prc_token := c_sql_pkg_table_trunc;
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

      -- Fill stage 1 for each source db
      FOR i IN g_l_distr_code.FIRST .. g_l_distr_code.LAST LOOP
         IF g_n_source_nk_flag = 0
        AND g_vc_col_pk_src IS NOT NULL THEN
            l_vc_col_pk_notnull :=
               p#frm#stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            l_sql_prc_token := c_sql_stg1_body_dedupl;
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'tableNameDupl'
             , g_vc_table_name_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
         ELSE
            -- If no deduplication is needed use normal insert statement
            l_sql_prc_token := c_sql_stg1_body_insert;
         END IF;

         -- Add optional increment retrieval statement
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'computeIncrementBound'
          , CASE
               WHEN g_vc_increment_column IS NOT NULL THEN
                  c_sql_stg1_body_incr
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'incrementColumn'
          , g_vc_increment_column
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableName'
          , g_vc_table_name_stage1
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tableNameStage2'
          , g_vc_table_name_stage2
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColAllTrg'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColAllSrc'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColDupl'
          , l_vc_col_dupl
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColPk'
          , g_vc_col_pk_src
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listColUtl'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'listExpUtl'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'dblink'
          , CASE
               WHEN g_l_dblink (i) IS NOT NULL THEN
                     '@'
                  || g_l_dblink (i)
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'owner'
          ,    g_l_owner_src (i)
            || '.'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'distracolumn'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  '#columnSourceDistribution#, '
               WHEN g_vc_partition_clause IS NOT NULL THEN
                  '#columnPartition#, '
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'distrValue'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     ''''
                  || g_l_distr_code (i)
                  || ''', '
               WHEN g_vc_partition_clause IS NOT NULL THEN
                     ' CASE WHEN TRIM( TRANSLATE ('
                  || g_vc_partition_clause
                  || ',''0123456789'',''          '')) IS NULL THEN TO_NUMBER('
                  || g_vc_partition_clause
                  || ') ELSE 0 END, '
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'distracode'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  g_l_distr_code (i)
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tablePartition'
          , NULL
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
          , 'backwardCompColumns'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'backwardCompValues'
          , NULL
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
       , c_sql_prc_param
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
       , c_sql_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_sql_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_sql_exception
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
       , 'prc_load_stage1'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_stage;

   PROCEDURE prc_create_prc_stage_p (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message          VARCHAR2 (32000)
                               :=    'Procedure load stage1 partition '
                                  || g_vc_package_main;
      l_sql_prc             CLOB;
      l_sql_prc_token       CLOB;
      l_sql_prc_buffer      CLOB;
      l_n_iter_begin        NUMBER;
      l_n_iter_end          NUMBER;
      l_n_increment_bound   NUMBER;
      l_d_increment_bound   DATE;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );

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
             ,    'prc_load_stage1_'
               || g_l_distr_code (i)
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc
             , 'prcParameters'
             , c_sql_prc_param
            );
            l_sql_pkg_head_buffer :=
                  l_sql_pkg_head_buffer
               || CHR (10)
               || l_sql_prc;
         END LOOP;
      ELSIF g_vc_partition_clause IS NOT NULL THEN
         FOR i IN 0 .. 9 LOOP
            -- Stage1 procedure head
            l_sql_prc := p#frm#ddls.c_template_prc_head;
            p#frm#ddls.prc_set_text_param (
               l_sql_prc
             , 'prcName'
             ,    'prc_load_stage1_p'
               || i
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc
             , 'prcParameters'
             , c_sql_prc_param
            );
            l_sql_pkg_head_buffer :=
                  l_sql_pkg_head_buffer
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
      ELSIF g_vc_partition_clause IS NOT NULL THEN
         l_n_iter_begin := 0;
         l_n_iter_end := 9;
      END IF;

      FOR i IN l_n_iter_begin .. l_n_iter_end LOOP
         l_sql_prc_buffer := c_sql_pkg_part_trunc;

         IF g_n_source_nk_flag = 0
        AND g_vc_col_pk_src IS NOT NULL THEN
            l_vc_col_pk_notnull :=
               p#frm#stag_meta.fct_get_column_list (
                  g_n_object_id
                , 'PK'
                , 'AND_NOTNULL'
               );
            l_sql_prc_buffer :=
                  l_sql_prc_buffer
               || CHR (10)
               || c_sql_stg1_body_dedupl;
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_buffer
             , 'tableNameDupl'
             , g_vc_table_name_dupl
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_buffer
             , 'notNullClause'
             , l_vc_col_pk_notnull
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_buffer
             , 'deduplRankClause'
             , g_vc_dedupl_rank_clause
            );
         ELSE
            l_sql_prc_buffer :=
                  l_sql_prc_buffer
               || CHR (10)
               || c_sql_stg1_body_insert;
         END IF;

         -- Add optional increment retrieval statement
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'computeIncrementBound'
          , CASE
               WHEN g_vc_increment_column IS NOT NULL THEN
                  c_sql_stg1_body_incr
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'incrementColumn'
          , g_vc_increment_column
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'tableName'
          , g_vc_table_name_stage1
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'listColAllTrg'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'listColAllSrc'
          , g_vc_col_all
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'listColDupl'
          , l_vc_col_dupl
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'listColPk'
          , g_vc_col_pk_src
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'backwardCompColumns'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'backwardCompValues'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'listColUtl'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'listExpUtl'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'dblink'
          , CASE
               WHEN g_l_distr_code.COUNT > 1
                AND g_l_dblink (i) IS NOT NULL THEN
                     '@'
                  || g_l_dblink (i)
               WHEN g_l_dblink.COUNT = 1
                AND g_l_dblink (1) IS NOT NULL THEN
                     '@'
                  || g_l_dblink (1)
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'owner'
          ,    CASE
                  WHEN g_l_distr_code.COUNT > 1 THEN
                     g_l_owner_src (i)
                  WHEN g_l_dblink.COUNT = 1 THEN
                     g_l_owner_src (1)
               END
            || '.'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'distracolumn'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  '#columnSourceDistribution#, '
               WHEN g_vc_partition_clause IS NOT NULL THEN
                  '#columnPartition#, '
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'distrValue'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                     ''''
                  || g_l_distr_code (i)
                  || ''', '
               WHEN g_vc_partition_clause IS NOT NULL THEN
                     TO_CHAR (i)
                  || ','
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'distracode'
          , CASE
               WHEN g_l_distr_code.COUNT > 1 THEN
                  g_l_distr_code (i)
               WHEN g_vc_partition_clause IS NOT NULL THEN
                  TO_CHAR (i)
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'partition'
          , CASE
               WHEN g_l_distr_code.COUNT > 1
                 OR g_vc_partition_clause IS NOT NULL THEN
                  TRIM (TO_CHAR (i))
               ELSE
                  'NULL'
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'tablePartition'
          ,    'PARTITION (p'
            || CASE
                  WHEN g_l_distr_code.COUNT > 1 THEN
                        '_'
                     || g_l_distr_code (i)
                  ELSE
                     TO_CHAR (i)
               END
            || ')'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'filterClause'
          ,    CASE
                  WHEN g_vc_partition_clause IS NOT NULL THEN
                        ' WHERE CASE WHEN TRIM( TRANSLATE ('
                     || g_vc_partition_clause
                     || ',''0123456789'',''          '')) IS NULL THEN TO_NUMBER('
                     || g_vc_partition_clause
                     || ') ELSE 0 END = '
                     || i
               END
            || CASE
                  WHEN g_vc_filter_clause IS NOT NULL THEN
                        CASE
                           WHEN g_vc_partition_clause IS NULL THEN
                              ' WHERE '
                           ELSE
                              ' AND '
                        END
                     || g_vc_filter_clause
               END
            || CASE
                  WHEN g_vc_increment_column IS NOT NULL THEN
                        CASE
                           WHEN g_vc_partition_clause IS NULL
                            AND g_vc_filter_clause IS NULL THEN
                              ' WHERE '
                           ELSE
                              ' AND '
                        END
                     || g_vc_increment_column
                     || ' > l_t_increment_bound'
               END
         );
         -- Put body in the generic prc template
         l_sql_prc := p#frm#ddls.c_template_prc_body;
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'prcParameters'
          , c_sql_prc_param
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
          , c_sql_initialize
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'prcFinalize'
          , c_sql_finalize
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc
          , 'exceptionHandling'
          , c_sql_exception
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
          ,    'prc_load_stage1_'
            || CASE
                  WHEN g_l_distr_code.COUNT > 1 THEN
                     g_l_distr_code (i)
                  ELSE
                        'p'
                     || i
               END
         );
         l_sql_pkg_body_buffer :=
               l_sql_pkg_body_buffer
            || CHR (10)
            || l_sql_prc;
      END LOOP;

      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_stage_p;

   PROCEDURE prc_create_prc_hist (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message           VARCHAR2 (32000)
                                :=    'Procedure load stage2 '
                                   || g_vc_package_main;
      l_sql_prc              CLOB;
      l_sql_prc_token        CLOB;
      l_sql_prc_token_iter   CLOB;
      l_sql_prc_buffer       CLOB;
      -- List of columns
      l_vc_col_all           p#frm#type.vc_max_plsql;
      l_vc_col_pk_2          p#frm#type.vc_max_plsql;
      l_vc_clause_on         p#frm#type.vc_max_plsql;
      l_vc_upd_clause_set    p#frm#type.vc_max_plsql;
      l_vc_clause_update     p#frm#type.vc_max_plsql;
      l_vc_col_nvl2          p#frm#type.vc_max_plsql;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );
      -- Set anonymizad column lists
      l_vc_set_anonymized := '';
      l_vc_col_anonymized := '';
      l_vc_fct_anonymized := '';
      -- ANONYMIZATION prc_set_anonymized_columns;
      -- Get list of pk columns of the stage 2 table
      l_vc_col_pk_2 :=
         p#frm#dict.fct_get_column_list (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage2
          , 'PK'
          , 'LIST_SIMPLE'
         );
      --
      -- HEAD
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_stage2'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      -- Stage2 delta procedure head
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_stage2_delta'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Get list of all columns
      l_vc_col_all :=
         p#frm#dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage1
          , g_vc_owner_stg
          , g_vc_table_name_stage2
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
         );                                                                                                                                                                -- In case the pk of stage 1 and stage 2 tables is not the same, write a warning log

      IF g_vc_col_pk = l_vc_col_pk_2
      OR (g_vc_col_pk IS NULL
      AND l_vc_col_pk_2 IS NULL) THEN
         p#frm#trac.log_info (
               'Source '
            || g_vc_source_code
            || ', Object '
            || g_vc_table_name_source
            || ' : Stage 1 and stage 2 have the same Natural Keys'
          , 'CHECK PK'
         );
      ELSE
         p#frm#trac.log_info (
               'Source '
            || g_vc_source_code
            || ', Object '
            || g_vc_table_name_source
            || ' : Stage 1 and stage 2 have different Natural Keys'
          , 'CHECK NK'
         );
      END IF;

      l_sql_prc_token := c_sql_stg1_body_stats_stg1;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableName'
       , g_vc_table_name_stage1
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'partition'
       , NULL
      );
      l_sql_prc_buffer := l_sql_prc_token;

      IF g_n_source_nk_flag = 0
     AND g_vc_col_pk IS NOT NULL THEN
         -- Analyse duplicates table
         l_sql_prc_token := c_sql_stg1_body_stats_dupl;
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

      -- Check stage2 table and truncate diff
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || c_sql_stg2_check;

      IF g_vc_col_pk IS NULL THEN
         -- If there is no natural key (tecnical PK) then use the alternate difference method
         l_vc_clause_on :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage1
             , g_vc_owner_stg
             , g_vc_table_name_stage2
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
             , g_vc_table_name_stage2
             , 'PK'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
            );
         l_vc_upd_clause_set :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage1
             , g_vc_owner_stg
             , g_vc_table_name_stage2
             , 'COMMON_NPK'
             , 'SET_ALIAS'
             , 'trg'
             , 'src'
            );
         l_vc_col_nvl2 :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage1
             , g_vc_owner_stg
             , g_vc_table_name_stage2
             , 'COMMON_ALL'
             , 'LIST_NVL2'
             , 'src'
             , 'trg'
            );
         l_vc_clause_update :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage1
             , g_vc_owner_stg
             , g_vc_table_name_stage2
             , 'COMMON_NPK'
             , 'OR_DECODE'
             , 'trg'
             , 'src'
            );
      END IF;

      IF g_vc_partition_clause IS NOT NULL THEN
         l_sql_prc_token := '';

         FOR i IN 0 .. 9 LOOP
            IF g_vc_col_pk IS NULL THEN
               l_sql_prc_token_iter :=
                     c_sql_stg2_body_diff_nonk
                  || c_sql_stg2_body_stg2_2dml;
            ELSE
               l_sql_prc_token_iter :=
                     c_sql_stg2_body_diff_nk
                  || c_sql_stg2_body_stg2_2dml;
            END IF;

            p#frm#dict.prc_set_text_param (
               l_sql_prc_token_iter
             , 'enableParallelDML'
             , CASE
                  WHEN l_vc_set_anonymized IS NOT NULL THEN
                     c_sql_enable_parallel_dml
                  ELSE
                     c_sql_disable_parallel_dml
               END
            );
            p#frm#dict.prc_set_text_param (
               l_sql_prc_token_iter
             , 'partition'
             , TO_CHAR (i)
            );
            p#frm#dict.prc_set_text_param (
               l_sql_prc_token_iter
             , 'tablePartitionStage1'
             ,    ' PARTITION (P'
               || i
               || ')'
            );
            p#frm#dict.prc_set_text_param (
               l_sql_prc_token_iter
             , 'tablePartitionStage2'
             ,    ' PARTITION (P'
               || i
               || ')'
            );
            l_sql_prc_token :=
                  l_sql_prc_token
               || CHR (10)
               || l_sql_prc_token_iter;
         END LOOP;
      ELSE
         IF g_vc_col_pk IS NULL THEN
            l_sql_prc_token :=
                  c_sql_stg2_body_diff_nonk
               || c_sql_stg2_body_stg2_2dml;
         ELSE
            l_sql_prc_token :=
                  c_sql_stg2_body_diff_nk
               || c_sql_stg2_body_stg2_2dml;
         END IF;

         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'enableParallelDML'
          , CASE
               WHEN l_vc_set_anonymized IS NOT NULL THEN
                  c_sql_enable_parallel_dml
               ELSE
                  c_sql_disable_parallel_dml
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , 'NULL'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tablePartitionStage1'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tablePartitionStage2'
          , NULL
         );
      END IF;

      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listColAllNVL2'
       , l_vc_col_nvl2
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'updateClause'
       , NVL (l_vc_clause_update, '1=0')
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listOnClause'
       , l_vc_clause_on
      );
      -- Set the matched clause of the merge statement. This exists only if there are non-NK columns to set
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'matchedClause'
       , CASE
            WHEN l_vc_upd_clause_set IS NOT NULL THEN
                  l_vc_upd_clause_set
               || CHR (10)
               || l_vc_set_anonymized
               || ', '
         END
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listColUtl'
       , c_sql_utl_columns
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listValUtl'
       , ', SYSDATE, ''I'''
      );
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || l_sql_prc_token
         || CHR (10)
         || c_sql_stg2_body_stats;
      -- Put all other code parameters
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'stgOwner'
       , g_vc_owner_stg
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableNameDiff'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableNameStage1'
       , g_vc_table_name_stage1
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableNameStage2'
       , g_vc_table_name_stage2
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'listColAll'
       , l_vc_col_all
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'listColTarget'
       ,    l_vc_col_all
         || CHR (10)
         || l_vc_col_anonymized
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'listColSource'
       ,    l_vc_col_all
         || CHR (10)
         || l_vc_fct_anonymized
      );
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
      -- Load stage 2 with table comparison
      --
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_sql_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_sql_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_sql_exception
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
       , 'operationClause'
       , ' IS NOT NULL'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_stage2'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- Load stage 2 without deletes
      --
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_sql_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_sql_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_sql_exception
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
       , 'operationClause'
       , ' <> ''D'''
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_stage2_delta'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_hist;

   PROCEDURE prc_create_prc_diff_to_hist (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message           VARCHAR2 (32000)
                                :=    'Procedure load diff-to-stage2 '
                                   || g_vc_package_main;
      l_sql_prc              CLOB;
      l_sql_prc_token        CLOB;
      l_sql_prc_token_iter   CLOB;
      l_sql_prc_buffer       CLOB;
      -- List of columns
      l_vc_col_all           p#frm#type.vc_max_plsql;
      l_vc_clause_on         p#frm#type.vc_max_plsql;
      l_vc_upd_clause_set    p#frm#type.vc_max_plsql;
      l_vc_ins_col_source    p#frm#type.vc_max_plsql;
      l_vc_ins_col_target    p#frm#type.vc_max_plsql;
   BEGIN
      p#frm#trac.log_info (
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
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_diff_to_stg2'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY
      --
      -- Get list of all columns
      l_vc_col_all :=
         p#frm#dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage1
          , g_vc_owner_stg
          , g_vc_table_name_stage2
          , 'COMMON_ALL'
          , 'LIST_SIMPLE'
         );                                                                                                                                                                -- In case the pk of stage 1 and stage 2 tables is not the same, write a warning log
      l_vc_ins_col_source :=
         p#frm#dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage1
          , g_vc_owner_stg
          , g_vc_table_name_stage2
          , 'COMMON_ALL'
          , 'LIST_ALIAS'
          , 'src'
         );
      l_vc_ins_col_target :=
         p#frm#dict.fct_get_column_subset (
            NULL
          , g_vc_owner_stg
          , g_vc_table_name_stage1
          , g_vc_owner_stg
          , g_vc_table_name_stage2
          , 'COMMON_ALL'
          , 'LIST_ALIAS'
          , 'trg'
         );

      IF g_vc_col_pk IS NULL THEN
         -- If there is no natural key (tecnical PK) then use the alternate difference method
         l_vc_clause_on :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage1
             , g_vc_owner_stg
             , g_vc_table_name_stage2
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
             , g_vc_table_name_stage2
             , 'PK'
             , 'AND_ALIAS'
             , 'trg'
             , 'src'
            );
         l_vc_upd_clause_set :=
            p#frm#dict.fct_get_column_subset (
               NULL
             , g_vc_owner_stg
             , g_vc_table_name_stage1
             , g_vc_owner_stg
             , g_vc_table_name_stage2
             , 'COMMON_NPK'
             , 'SET_ALIAS'
             , 'trg'
             , 'src'
            );
      END IF;

      IF g_vc_partition_clause IS NOT NULL THEN
         FOR i IN 0 .. 9 LOOP
            l_sql_prc_token_iter := c_sql_stg2_body_stg2_1dml;
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token_iter
             , 'enableParallelDML'
             , CASE
                  WHEN l_vc_set_anonymized IS NOT NULL THEN
                     c_sql_enable_parallel_dml
                  ELSE
                     c_sql_disable_parallel_dml
               END
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token_iter
             , 'partition'
             , TO_CHAR (i)
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token_iter
             , 'tablePartitionStage1'
             ,    ' PARTITION (P'
               || i
               || ')'
            );
            p#frm#ddls.prc_set_text_param (
               l_sql_prc_token_iter
             , 'tablePartitionStage2'
             ,    ' PARTITION (P'
               || i
               || ')'
            );
            l_sql_prc_token :=
                  l_sql_prc_token
               || CHR (10)
               || l_sql_prc_token_iter;
         END LOOP;
      ELSE
         l_sql_prc_token := c_sql_stg2_body_stg2_1dml;
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'enableParallelDML'
          , CASE
               WHEN l_vc_set_anonymized IS NOT NULL THEN
                  c_sql_enable_parallel_dml
               ELSE
                  c_sql_disable_parallel_dml
            END
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'partition'
          , 'NULL'
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tablePartitionStage1'
          , NULL
         );
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_token
          , 'tablePartitionStage2'
          , NULL
         );
      END IF;

      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'tableNameDiff'
       , g_vc_table_name_diff
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listColSource'
       ,    l_vc_ins_col_source
         || CHR (10)
         || l_vc_ins_anonymized
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listColTarget'
       ,    l_vc_ins_col_target
         || CHR (10)
         || l_vc_col_anonymized
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listOnClause'
       , l_vc_clause_on
      );
      -- Set the matched clause of the merge statement. This exists only if there are non-NK columns to set
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'matchedClause'
       , CASE
            WHEN l_vc_upd_clause_set IS NOT NULL THEN
                  l_vc_upd_clause_set
               || CHR (10)
               || l_vc_set_anonymized
               || ', '
         END
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listColUtl'
       , c_sql_utl_columns
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_token
       , 'listValUtl'
       , ', SYSDATE, ''I'''
      );
      l_sql_prc_buffer :=
            l_sql_prc_buffer
         || CHR (10)
         || l_sql_prc_token
         || CHR (10)
         || c_sql_stg2_body_stats;
      -- Put all other code parameters
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'stgOwner'
       , g_vc_owner_stg
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableNameStage1'
       , g_vc_table_name_stage1
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableNameStage2'
       , g_vc_table_name_stage2
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'listColAll'
       , l_vc_col_all
      );
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
      -- Load stage 2 with table comparison
      --
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'varList'
       , NULL
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcInitialize'
       , c_sql_initialize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcFinalize'
       , c_sql_finalize
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'exceptionHandling'
       , c_sql_exception
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcBody'
       , l_sql_prc_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_diff_to_stg2'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_diff_to_hist;

   PROCEDURE prc_create_prc_wrapper (
      p_b_tc_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_message       VARCHAR2 (32000)
                            :=    'Procedure wrapper '
                               || g_vc_package_main;
      l_sql_prc          CLOB;
      l_sql_prc_token    CLOB;
      l_sql_prc_buffer   CLOB;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );
      --
      -- HEAD for FULL load
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY for FULL load
      --
      l_sql_prc_buffer := c_sql_stag_wrapper;

      IF p_b_tc_only_flag THEN
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcStage1'
          , NULL
         );
      ELSE
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcStage1'
          , 'prc_load_stage1;'
         );
      END IF;

      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcStage2'
       , 'prc_load_stage2;'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_stage2
      );
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
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
       , 'prc_load'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- HEAD for DELTA load
      --
      l_sql_prc := p#frm#ddls.c_template_prc_head;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcName'
       , 'prc_load_delta'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
      );
      l_sql_pkg_head_buffer :=
            l_sql_pkg_head_buffer
         || CHR (10)
         || l_sql_prc;
      --
      -- BODY for DELTA load
      --
      l_sql_prc_buffer := c_sql_stag_wrapper;

      IF p_b_tc_only_flag THEN
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcStage1'
          , NULL
         );
      ELSE
         p#frm#ddls.prc_set_text_param (
            l_sql_prc_buffer
          , 'prcStage1'
          , 'prc_load_stage1;'
         );
      END IF;

      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'prcStage2'
       , 'prc_load_stage2_delta;'
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_prc_buffer
       , 'tableName'
       , g_vc_table_name_stage2
      );
      -- Put body in the generic prc template
      l_sql_prc := p#frm#ddls.c_template_prc_body;
      p#frm#ddls.prc_set_text_param (
         l_sql_prc
       , 'prcParameters'
       , c_sql_prc_param
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
       , 'prc_load_delta'
      );
      l_sql_pkg_body_buffer :=
            l_sql_pkg_body_buffer
         || CHR (10)
         || l_sql_prc;
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_prc_wrapper;

   PROCEDURE prc_compile_package_main (p_b_raise_flag BOOLEAN DEFAULT FALSE)
   IS
      l_vc_message   VARCHAR2 (32000)
                        :=    'Package compile '
                           || g_vc_package_main;
      l_sql_create   CLOB;
   BEGIN
      -- Package head
      p#frm#trac.log_info (
         l_vc_message
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
       , l_sql_pkg_head_buffer
      );
      -- Execute ddl for package head
      prc_set_tech_columns (l_sql_create);
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
      p#frm#trac.log_info (
         l_vc_message
       , 'Package head: End'
      );
      -- Package body
      p#frm#trac.log_info (
         l_vc_message
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
       , l_sql_pkg_body_buffer
      );
      p#frm#ddls.prc_set_text_param (
         l_sql_create
       , 'pkgName'
       , g_vc_package_main
      );
      -- Execute ddl for package body
      prc_set_tech_columns (l_sql_create);
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
      p#frm#trac.log_info (
         l_vc_message
       , 'Package body: End'
      );
   END prc_compile_package_main;

   PROCEDURE prc_create_package_main (
      p_b_tc_only_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   )
   IS
      l_vc_message   VARCHAR2 (32000)
                        :=    'Package create '
                           || g_vc_package_main;
      l_sql_create   CLOB;
   BEGIN
      p#frm#trac.log_info (
         l_vc_message
       , 'Begin'
      );
      l_sql_pkg_head_buffer := '';
      l_sql_pkg_body_buffer := '';

      IF NOT p_b_tc_only_flag THEN
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
      -- Trunc Stage 1 table
      prc_create_prc_trunc_stage (p_b_raise_flag);
      --
      -- Trunc Diff table
      prc_create_prc_trunc_diff (p_b_raise_flag);

      IF NOT p_b_tc_only_flag THEN
         --
         -- Initial load
         prc_create_prc_init (p_b_raise_flag);

         IF g_l_dblink.COUNT > 1
         OR g_vc_partition_clause IS NOT NULL THEN
            --
            -- Stage 1 load - single partitions
            prc_create_prc_stage_p (p_b_raise_flag);
         END IF;

         --
         -- Stage 1 load
         prc_create_prc_stage (p_b_raise_flag);
      END IF;

      --
      -- Stage 2 load
      prc_create_prc_hist (p_b_raise_flag);
      prc_create_prc_diff_to_hist (p_b_raise_flag);
      --
      -- Wrapper
      prc_create_prc_wrapper (
         p_b_tc_only_flag
       , p_b_raise_flag
      );
      --
      -- Compile package
      prc_compile_package_main (p_b_raise_flag);
      p#frm#trac.log_info (
         l_vc_message
       , 'End'
      );
   END prc_create_package_main;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: hist_ddl-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/hist_ddl/hist_ddl-impl.sql $';
END p#frm#hist_ddl;