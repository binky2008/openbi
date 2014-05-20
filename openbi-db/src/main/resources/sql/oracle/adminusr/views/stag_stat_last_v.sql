CREATE OR REPLACE VIEW p#frm#stag_stat_last_v
AS
     SELECT stage_source
          , stage_object
          , MIN (first_date) AS first_begin_date
          , MAX (CASE
                    WHEN stage_id = 2
                     AND stage_action = 'ANL' THEN
                       last_date
                 END)
               AS last_complete_date
          , SUM (CASE
                    WHEN stage_id = 1
                     AND stage_action = 'INS' THEN
                       stat_value
                    ELSE
                       0
                 END)
               AS stg1_insert_cnt
          , SUM (CASE
                    WHEN stage_id = 1
                     AND stage_action = 'INS' THEN
                       stat_duration
                    ELSE
                       0
                 END)
               AS stg1_insert_duration
          , SUM (CASE
                    WHEN stage_id = 1
                     AND stage_action = 'ANL' THEN
                       stat_duration
                    ELSE
                       0
                 END)
               AS stg1_analyze_duration
          , SUM (CASE
                    WHEN stage_id = 1 THEN
                       stat_duration
                    ELSE
                       0
                 END)
               AS stg1_duration
          , SUM (CASE
                    WHEN stage_id = 2
                     AND stage_action = 'IDT' THEN
                       stat_value
                    ELSE
                       0
                 END)
               AS stg2_insert_cnt
          , SUM (CASE
                    WHEN stage_id = 2
                     AND stage_action = 'IDT' THEN
                       stat_duration
                    ELSE
                       0
                 END)
               AS stg2_insert_duration
          , SUM (CASE
                    WHEN stage_id = 2
                     AND stage_action IN ('MDT', 'MDE', 'FDI', 'FUP', 'FIN') THEN
                       stat_value
                    ELSE
                       0
                 END)
               AS stg2_delta_cnt
          , SUM (CASE
                    WHEN stage_id = 2
                     AND stage_action IN ('MDT', 'MDE', 'FDI', 'FUP', 'FIN') THEN
                       stat_duration
                    ELSE
                       0
                 END)
               AS stg2_delta_duration
          , SUM (CASE
                    WHEN stage_id = 2
                     AND stage_action = 'ANL' THEN
                       stat_duration
                    ELSE
                       0
                 END)
               AS stg2_analyze_duration
          , SUM (CASE
                    WHEN stage_id = 2 THEN
                       stat_duration
                    ELSE
                       0
                 END)
               AS stg2_duration
       FROM (  SELECT sc.stag_source_code AS stage_source
                    , ob.stag_object_name AS stage_object
                    , ty.stag_stat_type_code AS stage_action
                    , st.stag_id AS stage_id
                    , SUM (st.stag_stat_value) AS stat_value
                    , ROUND (  (  MAX (st.update_date)
                                - MIN (st.create_date))
                             * 86400)
                         AS stat_duration
                    , MAX (st.update_date) AS last_date
                    , MIN (st.create_date) AS first_date
                 FROM (SELECT s.*
                            , ROW_NUMBER () OVER (PARTITION BY stag_object_id, stag_partition, stag_stat_type_id, stag_id ORDER BY create_date DESC) AS stat_rank
                         FROM p#frm#stag_stat_t s) st
                    , p#frm#stag_stat_type_t ty
                    , p#frm#stag_object_t ob
                    , p#frm#stag_source_t sc
                WHERE st.stag_stat_type_id = ty.stag_stat_type_id
                  AND st.stag_object_id = ob.stag_object_id
                  AND ob.stag_source_id = sc.stag_source_id
                  AND st.stag_stat_value IS NOT NULL
                  AND st.stat_rank = 1
             GROUP BY sc.stag_source_code
                    , ob.stag_object_name
                    , ty.stag_stat_type_code
                    , st.stag_id)
   GROUP BY stage_source
          , stage_object;