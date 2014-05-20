CREATE OR REPLACE VIEW p#frm#stag_size_v
AS
     SELECT sc.stag_source_code
          , ob.stag_object_id
          , ob.stag_object_name
          , si.stag_table_name
          , si.stag_num_rows
          , si.stag_bytes
          , si.create_date
       FROM p#frm#stag_size_t si
          , p#frm#stag_object_t ob
          , p#frm#stag_source_t sc
      WHERE si.stag_object_id = ob.stag_object_id
        AND ob.stag_source_id = sc.stag_source_id
   ORDER BY si.create_date DESC;