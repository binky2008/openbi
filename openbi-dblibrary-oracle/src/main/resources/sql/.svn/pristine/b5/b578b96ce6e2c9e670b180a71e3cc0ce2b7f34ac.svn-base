CREATE OR REPLACE VIEW p#frm#stag_column_v
AS
     SELECT sc.stag_source_id
          , sc.stag_source_code
          , ob.stag_object_id
          , ob.stag_object_name
          , ob.stag_source_nk_flag
          , co.stag_column_id
          , co.stag_column_pos
          , co.stag_column_name
          , co.stag_column_name_map
          , co.stag_column_comment
          , co.stag_column_def
          , co.stag_column_def_src
          , co.stag_column_nk_pos
          , co.stag_column_hist_flag
          , co.stag_column_edwh_flag
          , co.update_date
       FROM p#frm#stag_column_t co
          , p#frm#stag_object_t ob
          , p#frm#stag_source_t sc
      WHERE ob.stag_object_id = co.stag_object_id
        AND ob.stag_source_id = sc.stag_source_id
   ORDER BY sc.stag_source_code
          , ob.stag_object_name
          , co.stag_column_pos;