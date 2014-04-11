CREATE OR REPLACE PACKAGE stmt
   AUTHID CURRENT_USER
AS
   /**
   * Templates for standard ddls
   * APIs to construct list of columns and column definitions
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
   c_spec_version        CONSTANT VARCHAR2 (1024) := '$Id: $';
   /**
   * Package spec repository URL.
   */
   c_spec_url            CONSTANT VARCHAR2 (1024) := '$HeadURL: $';
   /**
   * Package body version string.
   */
   c_body_version                 VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                     VARCHAR2 (1024);
   --
   --
   -- Copy the content of a source table into a target table
   c_sql_insert_copy              CLOB := '
        INSERT /*+APPEND*/ INTO #targetIdentifier# #partition# (
               #utlColumnList#
               #sourceDBColumnName#
               #targetColumnList#)
        SELECT #utlValueList#
               #sourceDBIdentifier#
               #sourceColumnList#
          FROM #sourceIdentifier#
               #filterClause#;';
   --
   -- Copy the content of a source table into a target table
   -- deduplicating source values among a defined natural key
   c_sql_insert_dedupl            CLOB := '
        INSERT /*+APPEND*/
          WHEN row_rank = 1
           AND #notNullClause#
          THEN INTO #targetIdentifier# #tablePartition# (
                #utlColumnList#
                #sourceDBColumnName#
                #targetColumnList#)
             VALUES (
                #utlValueList#
                #sourceDBIdentifier#
                #sourceColumnList#)
          ELSE INTO #deduplIdentifier# #tablePartition# (
                #sourceDBColumnName#
                #deduplColumnList#)
             VALUES (
                #sourceDBIdentifier#
                #deduplColumnList#)
         SELECT #deduplColumnList#
              , ROW_NUMBER () over (PARTITION BY #nkColumnList# #deduplRankClause#) AS row_rank
           FROM #sourceIdentifier#
                #filterClause#;';
   --
   -- Store the difference between 2 tables
   c_sql_insert_diff_with_nk      CLOB := '
      INSERT
        INTO #diffIdentifier# #targetPartition# (
            #targetColumnList#
          , #utlColumnList#)
        SELECT
            #targetColumnList#
          , #utlColumnList#
        FROM (SELECT
                 #nvl2columnList#
                , CASE
                        WHEN src.rowid IS NOT NULL
                        AND trg.rowid  IS NULL
                        THEN ''I'' -- new row in src
                        WHEN src.rowid       IS NULL
                        AND trg.rowid        IS NOT NULL
                        AND trg.#dmlOpColumnName# <> ''D''
                        THEN ''D'' -- row was deleted in src
                        WHEN src.rowid      IS NOT NULL
                        AND trg.rowid       IS NOT NULL
                        AND trg.#dmlOpColumnName# = ''D''
                        THEN ''R'' -- row was deleted and now reappeared
                        WHEN src.rowid IS NOT NULL
                        AND trg.rowid  IS NOT NULL
                        AND (#updateClause#)
                        THEN ''U''
                        ELSE NULL -- nothing to be done
                    END AS #dmlOpColumnName#
                  , trg.#validFromColumnName#
                  , trg.#validToColumnName#
                FROM #sourceIdentifier# #sourcePartition# src
                #joinType# OUTER JOIN #targetIdentifier# #targetPartition# trg
                ON    #joinClause#)
        WHERE
            #dmlOpColumnName# IS NOT NULL;';
   --
   --
   -- Diff token of the staging 2 procedure - nk non-present
   c_sql_insert_diff_without_nk   CLOB := '
      INSERT
        INTO #diffIdentifier# #targetPartition# (
            #targetColumnList#
          , #utlColumnList#)
        SELECT
            #targetColumnList#
          , #utlColumnList#
        FROM (SELECT #targetColumnList#
             , CASE
                  WHEN cnt_in_src > 0
                  AND cnt_in_dst = 0
                     THEN ''I''                                                                                                                                                          -- new row in src
                  WHEN cnt_in_src > 0
                  AND cnt_in_dst > 0
                  AND #dmlOpColumnName# = ''D''
                     THEN ''R''
                  WHEN cnt_in_src = 0
                  AND cnt_in_dst > 0
                  AND #dmlOpColumnName# <> ''D''
                     THEN ''D''
                  ELSE NULL
               END AS #dmlOpColumnName#
             , #validFromColumnName#
             , #validToColumnName#
          FROM (SELECT   #targetColumnList#
                       , MAX (#dmlOpColumnName#) AS #dmlOpColumnName#
                       , MAX (#validFromColumnName#) AS #validFromColumnName#
                       , MAX (#validToColumnName#) AS #validToColumnName#
                       , COUNT (rowid_src) AS cnt_in_src
                       , COUNT (rowid_dst) AS cnt_in_dst
                    FROM (SELECT #targetColumnList#
                               , NULL AS #columnTimestamp#
                               , NULL AS #dmlOpColumnName#
                               , ROWID AS rowid_src
                               , NULL AS rowid_dst
                            FROM #sourceIdentifier# #sourcePartition#
                          UNION ALL
                          SELECT #targetColumnList#
                               , #columnTimestamp#
                               , #dmlOpColumnName# AS #dmlOpColumnName#
                               , NULL AS rowid_src
                               , ROWID AS rowid_dst
                            FROM #targetIdentifier# #targetPartition#)
                GROUP BY #targetColumnList#))
        WHERE
            #dmlOpColumnName# #operationClause#;';
   --
   --
   -- Merge token of the staging 2 procedure - 1 single statement
   c_sql_reconcile_merge          CLOB := '
      MERGE /*+APPEND*/
         INTO #targetIdentifier# trg
      USING
            (SELECT #dmlOpColumnName#
                  , #targetColumnList#
               FROM #tableNameDiff# #tablePartitionStage2#) src
                 ON (#listOnClause#)
        WHEN MATCHED THEN
             UPDATE
                 SET #matchedClause#
                      trg.#dmlOpColumnName# = src.#dmlOpColumnName#
                    , trg.#validFromColumnName# = SYSDATE
        WHEN NOT MATCHED THEN
             INSERT (
                #targetColumnList#
              , #utlColumnList#)
             )
             VALUES (
                #sourceColumnList#
                #utlValueList#
             );';
   --
   --
   -- Merge token of the staging 2 procedure - 2 separate statement
   c_sql_reconcile_update         CLOB := '
      MERGE /*+APPEND*/
         INTO #tableNameStage2# trg
      USING
            (SELECT #columnDmlOperation#
                  , #listColAll#
               FROM #tableNameDiff# #tablePartitionStage2#
              WHERE #columnDmlOperation# IN (''U'', ''R'', ''D'')) src
                  ON (#listOnClause#)
        WHEN MATCHED THEN
             UPDATE
                 SET #matchedClause#
                      trg.#columnDmlOperation# = src.#columnDmlOperation#
                    , trg.#columnTimestamp# = SYSDATE;';
   --
   --
   -- Merge token of the staging 2 procedure - 2 separate statement
   c_sql_reconcile_insert         CLOB := '
      INSERT /*+APPEND*/ INTO #tableNameStage2# #tablePartitionStage2# (
                              #listColTarget#
                            , #listColUtl#)
                              SELECT #listColSource#
                                   , #listValUtl#
                                FROM #tableNameDiff# #tablePartitionStage2#
                               WHERE #columnDmlOperation# = ''I'';';

   /**
   * Substitute a parameter (#parameter_name#) with a text
   *
   * @param p_vc_code_string     Parameterized string
   * @param p_vc_param_name      Name of the parameter, surrounded by "#"
   * @param p_vc_param_value     Substitute text
   */
   PROCEDURE prc_set_text_param (
      p_vc_code_string   IN OUT CLOB
    , p_vc_param_name    IN     TYPE.vc_obj_plsql
    , p_vc_param_value   IN     CLOB
   );

   PROCEDURE prc_get_identifier (
      p_vc_dblink         VARCHAR2
    , p_vc_schema_name    VARCHAR2
    , p_vc_object_name    VARCHAR2
   );
END stmt;