CREATE OR REPLACE VIEW stg_stat_last_v
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
       FROM (SELECT   sc.stg_source_code AS stage_source
                    , ob.stg_object_name AS stage_object
                    , ty.stg_stat_type_code AS stage_action
                    , st.stg_id AS stage_id
                    , SUM (st.stg_stat_value) AS stat_value
                    , ROUND ((MAX (st.update_date) - MIN (st.create_date)) * 86400) AS stat_duration
                    , MAX (st.update_date) AS last_date
                    , MIN (st.create_date) AS first_date
                 FROM (SELECT s.*
                            , ROW_NUMBER () OVER (PARTITION BY stg_object_id, stg_partition, stg_stat_type_id, stg_id ORDER BY create_date DESC) AS stat_rank
                         FROM stg_stat_t s) st
                    , stg_stat_type_t ty
                    , stg_object_t ob
                    , stg_source_t sc
                WHERE st.stg_stat_type_id = ty.stg_stat_type_id
                  AND st.stg_object_id = ob.stg_object_id
                  AND ob.stg_source_id = sc.stg_source_id
                  AND st.stg_stat_value IS NOT NULL
                  AND st.stat_rank = 1
             GROUP BY sc.stg_source_code
                    , ob.stg_object_name
                    , ty.stg_stat_type_code
                    , st.stg_id)
   GROUP BY stage_source
          , stage_object;

COMMENT ON TABLE stg_stat_last_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_stat_last_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_stat_last_v'
                                 , 'stg_stat_last_v'
                                 , TRUE
                                  );
END;
/