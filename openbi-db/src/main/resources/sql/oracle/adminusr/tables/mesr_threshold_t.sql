BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'mesr_threshold'
    , 'mesr_keyfigure_id NUMBER,
       mesr_threshold_type CHAR(1) DEFAULT ''A'' CHECK (mesr_threshold_type IN (''A'',''I'')),
       mesr_threshold_from DATE,
       mesr_threshold_to DATE,
       mesr_threshold_min FLOAT,
       mesr_threshold_max FLOAT'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;