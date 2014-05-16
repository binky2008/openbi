CREATE OR REPLACE PACKAGE BODY p#frm#stag_stat
AS
   /**
   * $Author: nmarangoni $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   FUNCTION prc_stat_begin (
      p_vc_source_code       VARCHAR2
    , p_vc_object_name       VARCHAR2
    , p_n_partition          NUMBER DEFAULT NULL
    , p_vc_stat_type_code    VARCHAR2 DEFAULT NULL
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
         NULL;
      EXCEPTION
         WHEN OTHERS THEN
            NULL;
      END;

      DBMS_APPLICATION_INFO.set_module (
            'OBJECT '
         || p_vc_object_name
       ,    'STAGE PART'
         || p_n_partition
         || ' '
         || p_vc_stat_type_code
      );

      SELECT MIN (stag_object_id)
        INTO l_n_object_id
        FROM stag_source_t s
           , stag_object_t o
       WHERE s.stag_source_id = o.stag_source_id
         AND s.stag_source_code = p_vc_source_code
         AND o.stag_object_name = p_vc_object_name;

      SELECT MIN (stag_stat_type_id)
        INTO l_n_stat_type_id
        FROM stag_stat_type_t
       WHERE stag_stat_type_code = p_vc_stat_type_code;

      INSERT INTO stag_stat_t (
                     stag_object_id
                   , stag_partition
                   , stag_load_id
                   , stag_stat_type_id
                   , stag_stat_sid
                  )
           VALUES (
                     l_n_object_id
                   , p_n_partition
                   , l_n_load_id
                   , l_n_stat_type_id
                   , TO_NUMBER (SYS_CONTEXT (
                                   'USERENV'
                                 , 'SESSIONID'
                                ))
                  )
        RETURNING stag_stat_id
             INTO l_n_result;

      COMMIT;
      RETURN l_n_result;
   END prc_stat_begin;

   PROCEDURE prc_stat_end (
      p_n_stat_id       NUMBER
    , p_n_stat_value    NUMBER DEFAULT 0
    , p_n_stat_error    NUMBER DEFAULT 0
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      UPDATE stag_stat_t
         SET stag_stat_value = p_n_stat_value
           , stag_stat_error = p_n_stat_error
       WHERE stag_stat_id = p_n_stat_id;

      COMMIT;
   END prc_stat_end;

   PROCEDURE prc_stat_purge
   IS
   BEGIN
      DELETE stag_stat_t
       WHERE stag_stat_value IS NULL;

      COMMIT;
   END prc_stat_purge;

   PROCEDURE prc_size_store (
      p_vc_source_code    VARCHAR2
    , p_vc_object_name    VARCHAR2
    , p_vc_table_name     VARCHAR2
   )
   IS
   BEGIN
      INSERT INTO stag_size_t (
                     stag_object_id
                   , stag_table_name
                   , stag_num_rows
                   , stag_bytes
                  )
           SELECT ob.stag_object_id
                , p_vc_table_name
                , tb.num_rows
                , SUM (sg.bytes)
             FROM stag_object_t ob
                , stag_source_t sr
                , user_tables tb
                , user_segments sg
            WHERE ob.stag_source_id = sr.stag_source_id
              AND sr.stag_source_code = p_vc_source_code
              AND ob.stag_object_name = p_vc_object_name
              AND tb.table_name = p_vc_table_name
              AND sg.segment_name = p_vc_table_name
         GROUP BY ob.stag_object_id
                , p_vc_table_name
                , tb.num_rows;

      COMMIT;
   END prc_size_store;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: $';
   c_body_url := '$HeadURL: $';
END p#frm#stag_stat;