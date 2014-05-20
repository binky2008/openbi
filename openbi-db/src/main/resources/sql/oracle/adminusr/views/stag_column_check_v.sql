CREATE OR REPLACE VIEW p#frm#stag_column_check_v
AS
     SELECT sc.stag_source_id
          , sc.stag_source_code
          , ob.stag_object_id
          , ob.stag_object_name
          , ob.stag_source_nk_flag
          , co.stag_column_id
          , co.stag_column_name
          , co.stag_column_name_map
          , co.stag_column_comment
          , co.stag_column_edwh_flag
          , co.stag_column_stag_pos
          , co.stag_column_stag_def
          , co.stag_column_stag_nk_pos
          , co.stag_column_src_pos
          , co.stag_column_src_def
          , co.stag_column_src_nk_pos
          , co.update_date
       FROM (SELECT NVL (c.stag_object_id, k.stag_object_id) AS stag_object_id
                  , c.stag_column_id
                  , NVL (c.stag_column_name, k.stag_column_name) AS stag_column_name
                  , c.stag_column_name_map
                  , c.stag_column_comment
                  , c.stag_column_edwh_flag
                  , c.stag_column_pos AS stag_column_stag_pos
                  , c.stag_column_def AS stag_column_stag_def
                  , c.stag_column_nk_pos AS stag_column_stag_nk_pos
                  , k.stag_column_pos AS stag_column_src_pos
                  , k.stag_column_def AS stag_column_src_def
                  , k.stag_column_nk_pos AS stag_column_src_nk_pos
                  , c.update_date
               FROM p#frm#stag_column_check_t k
                    FULL OUTER JOIN p#frm#stag_column_t c
                       ON c.stag_object_id = k.stag_object_id
                      AND c.stag_column_name = k.stag_column_name) co
          , p#frm#stag_object_t ob
          , p#frm#stag_source_t sc
      WHERE ob.stag_object_id = co.stag_object_id
        AND ob.stag_source_id = sc.stag_source_id
   ORDER BY sc.stag_source_code
          , ob.stag_object_name
          , NVL (co.stag_column_stag_pos, co.stag_column_src_pos);