CREATE OR REPLACE VIEW qc_meta_v
AS
   SELECT   l.sys_layer_code
          , l.sys_layer_name
          , e.sys_entity_code
          , e.sys_entity_name
          , en.sys_environment_code
          , en.sys_environment_name
          , c.qc_case_code
          , c.qc_case_name
          , s.qc_step_code
          , s.qc_step_name
          , s.qc_step_sql
          , k.qc_keyfigure_code
          , k.qc_keyfigure_name
          , t.qc_threshold_type
          , t.qc_threshold_from
          , t.qc_threshold_to
          , t.qc_threshold_min
          , t.qc_threshold_max
       FROM qc_case_t c
          , qc_step_t s
          , qc_keyfigure_t k
          , qc_threshold_t t
          , sys_layer_t l
          , sys_entity_t e
          , sys_environment_t en
      WHERE c.qc_case_id = s.qc_case_id(+)
        AND s.qc_step_id = k.qc_step_id(+)
        AND k.qc_keyfigure_id = t.qc_keyfigure_id(+)
        AND c.sys_layer_id = l.sys_layer_id(+)
        AND c.sys_entity_id = e.sys_entity_id(+)
        AND c.sys_environment_id = en.sys_environment_id(+)
   ORDER BY c.qc_case_id DESC
          , s.qc_step_id DESC
          , k.qc_keyfigure_id DESC
          , t.qc_threshold_id DESC;

COMMENT ON TABLE qc_meta_v IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON qc_meta_v TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('qc_meta_v'
                                 , 'qc_meta_v'
                                 , TRUE
                                  );
END;
/