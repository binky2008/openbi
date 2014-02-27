CREATE OR REPLACE PACKAGE BODY pkg_etl_stage_stat
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2011-10-05 13:37:55 +0200 (Mi, 05 Okt 2011) $
    * $Revision: 1566 $
    * $Id: pkg_etl_stage_stat-impl.sql 1566 2011-10-05 11:37:55Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_stat/pkg_etl_stage_stat-impl.sql $
    */
   PROCEDURE prc_set_load_id
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
   END prc_set_load_id;

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
         l_n_load_id    := TO_NUMBER (pkg_utl_parameter.get_parameter ('STAGE_LOAD_ID'
                                                                     , 'STAGE'
                                                                     , 'STAGE'
                                                                      ));
      EXCEPTION
         WHEN OTHERS
         THEN
            NULL;
      END;

      DBMS_APPLICATION_INFO.set_module ('OBJECT ' || p_vc_object_name, 'STAGE' || p_n_stage_id || ' ' || ' PART' || p_n_partition || ' ' || p_vc_stat_type_code);

      SELECT MIN (etl_stage_object_id)
        INTO l_n_object_id
        FROM etl_stage_source_t s
           , etl_stage_object_t o
       WHERE s.etl_stage_source_id = o.etl_stage_source_id
         AND s.etl_stage_source_code = p_vc_source_code
         AND o.etl_stage_object_name = p_vc_object_name;

      SELECT MIN (etl_stage_stat_type_id)
        INTO l_n_stat_type_id
        FROM etl_stage_stat_type_t
       WHERE etl_stage_stat_type_code = p_vc_stat_type_code;

      INSERT INTO etl_stage_stat_t
                  (etl_stage_id
                 , etl_stage_object_id
                 , etl_stage_partition
                 , etl_stage_load_id
                 , etl_stage_stat_type_id
                 , etl_stage_stat_gui
                 , etl_stage_stat_sid
                  )
           VALUES (p_n_stage_id
                 , l_n_object_id
                 , p_n_partition
                 , l_n_load_id
                 , l_n_stat_type_id
                 , pkg_utl_log.get_di_gui
                 , TO_NUMBER (SYS_CONTEXT ('USERENV', 'SESSIONID'))
                  )
        RETURNING etl_stage_stat_id
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
      UPDATE etl_stage_stat_t
         SET etl_stage_stat_value = p_n_stat_value
           , etl_stage_stat_error = p_n_stat_error
       WHERE etl_stage_stat_id = p_n_stat_id;

      COMMIT;
   END prc_stat_end;

   PROCEDURE prc_stat_purge
   IS
   BEGIN
      DELETE      etl_stage_stat_t
            WHERE etl_stage_stat_value IS NULL;

      COMMIT;
   END prc_stat_purge;

   PROCEDURE prc_size_store (
      p_vc_source_code   VARCHAR2
    , p_vc_object_name   VARCHAR2
    , p_vc_table_name    VARCHAR2
   )
   IS
   BEGIN
      INSERT INTO etl_stage_size_t
                  (etl_stage_object_id
                 , etl_stage_table_name
                 , etl_stage_num_rows
                 , etl_stage_bytes
                  )
         SELECT   ob.etl_stage_object_id
                , p_vc_table_name
                , tb.num_rows
                , SUM (sg.BYTES)
             FROM etl_stage_object_t ob
                , etl_stage_source_t sr
                , user_tables tb
                , user_segments sg
            WHERE ob.etl_stage_source_id = sr.etl_stage_source_id
              AND sr.etl_stage_source_code = p_vc_source_code
              AND ob.etl_stage_object_name = p_vc_object_name
              AND tb.table_name = p_vc_table_name
              AND sg.segment_name = p_vc_table_name
         GROUP BY ob.etl_stage_object_id
                , p_vc_table_name
                , tb.num_rows;

      COMMIT;
   END prc_size_store;
/**
 * Package initialization
 */
BEGIN
   c_body_version    := '$Id: pkg_etl_stage_stat-impl.sql 1566 2011-10-05 11:37:55Z nmarangoni $';
   c_body_url        := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_stat/pkg_etl_stage_stat-impl.sql $';
END pkg_etl_stage_stat;
/

SHOW errors

BEGIN
   pkg_utl_ddl.prc_create_synonym ('pkg_etl_stage_stat'
                                 , 'pkg_etl_stage_stat'
                                 , TRUE
                                  );
END;
/

SHOW errors