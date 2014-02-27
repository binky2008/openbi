CREATE OR REPLACE VIEW etl_stage_stat_last_v
AS
   SELECT   stage_source
          , stage_object
          , MIN (first_date) AS first_begin_date
          , MAX (CASE
                    WHEN stage_id = 2
                    AND stage_action = 'ANL'
                       THEN last_date
                 END) AS last_complete_date
          , SUM (CASE
                    WHEN stage_id = 1
                    AND stage_action = 'INS'
                       THEN stat_value
                    ELSE 0
                 END) AS stg1_insert_cnt
          , SUM (CASE
                    WHEN stage_id = 1
                    AND stage_action = 'INS'
                       THEN stat_duration
                    ELSE 0
                 END) AS stg1_insert_duration
          , SUM (CASE
                    WHEN stage_id = 1
                    AND stage_action = 'ANL'
                       THEN stat_duration
                    ELSE 0
                 END) AS stg1_analyze_duration
          , SUM (CASE
                    WHEN stage_id = 1
                       THEN stat_duration
                    ELSE 0
                 END) AS stg1_duration
          , SUM (CASE
                    WHEN stage_id = 2
                    AND stage_action = 'IDT'
                       THEN stat_value
                    ELSE 0
                 END) AS stg2_insert_cnt
          , SUM (CASE
                    WHEN stage_id = 2
                    AND stage_action = 'IDT'
                       THEN stat_duration
                    ELSE 0
                 END) AS stg2_insert_duration
          , SUM (CASE
                    WHEN stage_id = 2
                    AND stage_action IN ('MDT', 'MDE', 'FDI', 'FUP', 'FIN')
                       THEN stat_value
                    ELSE 0
                 END) AS stg2_delta_cnt
          , SUM (CASE
                    WHEN stage_id = 2
                    AND stage_action IN ('MDT', 'MDE', 'FDI', 'FUP', 'FIN')
                       THEN stat_duration
                    ELSE 0
                 END) AS stg2_delta_duration
          , SUM (CASE
                    WHEN stage_id = 2
                    AND stage_action = 'ANL'
                       THEN stat_duration
                    ELSE 0
                 END) AS stg2_analyze_duration
          , SUM (CASE
                    WHEN stage_id = 2
                       THEN stat_duration
                    ELSE 0
                 END) AS stg2_duration
       FROM (SELECT   sc.etl_stage_source_code AS stage_source
                    , ob.etl_stage_object_name AS stage_object
                    , ty.etl_stage_stat_type_code AS stage_action
                    , st.etl_stage_id AS stage_id
                    , SUM (st.etl_stage_stat_value) AS stat_value
                    , ROUND ((MAX (st.update_date) - MIN (st.create_date)) * 86400) AS stat_duration
                    , MAX (st.update_date) AS last_date
                    , MIN (st.create_date) AS first_date
                 FROM (SELECT s.*
                            , ROW_NUMBER () OVER (PARTITION BY etl_stage_object_id, etl_stage_partition, etl_stage_stat_type_id, etl_stage_id ORDER BY create_date DESC) AS stat_rank
                         FROM etl_stage_stat_t s) st
                    , etl_stage_stat_type_t ty
                    , etl_stage_object_t ob
                    , etl_stage_source_t sc
                WHERE st.etl_stage_stat_type_id = ty.etl_stage_stat_type_id
                  AND st.etl_stage_object_id = ob.etl_stage_object_id
                  AND ob.etl_stage_source_id = sc.etl_stage_source_id
                  AND st.etl_stage_stat_value IS NOT NULL
                  AND st.stat_rank = 1
             GROUP BY sc.etl_stage_source_code
                    , ob.etl_stage_object_name
                    , ty.etl_stage_stat_type_code
                    , st.etl_stage_id)
   GROUP BY stage_source
          , stage_object;

COMMENT ON TABLE etl_stage_stat_last_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_stat_last_v TO PUBLIC;

BEGIN
   pkg_utl_ddl.prc_create_synonym ('etl_stage_stat_last_v'
                                 , 'etl_stage_stat_last_v'
                                 , TRUE
                                  );
END;
/