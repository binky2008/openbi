CREATE OR REPLACE PACKAGE BODY stg_stat
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2011-10-05 13:37:55 +0200 (Mi, 05 Okt 2011) $
    * $Revision: 1566 $
    * $Id: stg_stat-impl.sql 1566 2011-10-05 11:37:55Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_stat/stg_stat-impl.sql $
    */
   /*PROCEDURE prc_set_load_id
   IS
      l_n_result   NUMBER;
   BEGIN
      l_n_result    := pkg_utl_parameter.set_parameter ('STAGE_LOAD_ID'
                                                      , TO_NUMBER (TO_CHAR (SYSDATE, 'yyyymmddhh24miss'))
                                                      , 'STAGE'
                                                      , 'STAGE'
                                                       );
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END prc_set_load_id;*/

   FUNCTION prc_stat_begin (
      p_vc_source_code      VARCHAR2
    , p_vc_object_name      VARCHAR2
    , p_n_stage_id          NUMBER DEFAULT NULL
    , p_n_partition         NUMBER DEFAULT NULL
    , p_vc_stat_type_code   VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_n_stat_type_id   NUMBER;
      l_n_object_id      NUMBER;
      l_n_result         NUMBER;
      l_n_load_id        NUMBER;
   BEGIN
      BEGIN
         /*l_n_load_id    := TO_NUMBER (pkg_utl_parameter.get_parameter ('STAGE_LOAD_ID'
                                                                     , 'STAGE'
                                                                     , 'STAGE'
                                                                      ));*/
        null;
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;

      DBMS_APPLICATION_INFO.set_module ('OBJECT ' || p_vc_object_name, 'STAGE' || p_n_stage_id || ' ' || ' PART' || p_n_partition || ' ' || p_vc_stat_type_code);

      SELECT MIN (stg_object_id)
        INTO l_n_object_id
        FROM stg_source_t s
           , stg_object_t o
       WHERE s.stg_source_id = o.stg_source_id
         AND s.stg_source_code = p_vc_source_code
         AND o.stg_object_name = p_vc_object_name;

      SELECT MIN (stg_stat_type_id)
        INTO l_n_stat_type_id
        FROM stg_stat_type_t
       WHERE stg_stat_type_code = p_vc_stat_type_code;

      INSERT INTO stg_stat_t
                  (stg_id
                 , stg_object_id
                 , stg_partition
                 , stg_load_id
                 , stg_stat_type_id
                 , stg_stat_sid
                  )
           VALUES (p_n_stage_id
                 , l_n_object_id
                 , p_n_partition
                 , l_n_load_id
                 , l_n_stat_type_id
                 , TO_NUMBER (SYS_CONTEXT ('USERENV', 'SESSIONID'))
                  )
        RETURNING stg_stat_id
             INTO l_n_result;

      COMMIT;
      RETURN l_n_result;
   END prc_stat_begin;

   PROCEDURE prc_stat_end (
      p_n_stat_id      NUMBER
    , p_n_stat_value   NUMBER DEFAULT 0
    , p_n_stat_error   NUMBER DEFAULT 0
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      UPDATE stg_stat_t
         SET stg_stat_value = p_n_stat_value
           , stg_stat_error = p_n_stat_error
       WHERE stg_stat_id = p_n_stat_id;

      COMMIT;
   END prc_stat_end;

   PROCEDURE prc_stat_purge
   IS
   BEGIN
      DELETE      stg_stat_t
            WHERE stg_stat_value IS NULL;

      COMMIT;
   END prc_stat_purge;

   PROCEDURE prc_size_store (
      p_vc_source_code   VARCHAR2
    , p_vc_object_name   VARCHAR2
    , p_vc_table_name    VARCHAR2
   )
   IS
   BEGIN
      INSERT INTO stg_size_t
                  (stg_object_id
                 , stg_table_name
                 , stg_num_rows
                 , stg_bytes
                  )
         SELECT   ob.stg_object_id
                , p_vc_table_name
                , tb.num_rows
                , SUM (sg.BYTES)
             FROM stg_object_t ob
                , stg_source_t sr
                , user_tables tb
                , user_segments sg
            WHERE ob.stg_source_id = sr.stg_source_id
              AND sr.stg_source_code = p_vc_source_code
              AND ob.stg_object_name = p_vc_object_name
              AND tb.table_name = p_vc_table_name
              AND sg.segment_name = p_vc_table_name
         GROUP BY ob.stg_object_id
                , p_vc_table_name
                , tb.num_rows;

      COMMIT;
   END prc_size_store;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: stg_stat-impl.sql 1566 2011-10-05 11:37:55Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/stg_stat/stg_stat-impl.sql $';
END stg_stat;
/

SHOW errors

BEGIN
   ddl.prc_create_synonym ('stg_stat'
                                 , 'stg_stat'
                                 , TRUE
                                  );
END;
/

SHOW errors