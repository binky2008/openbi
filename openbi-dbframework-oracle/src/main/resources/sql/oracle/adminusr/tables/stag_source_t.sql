BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'stag_source'
    , 'stag_source_code VARCHAR2(10),
		stag_source_prefix VARCHAR2(10),
		stag_source_name VARCHAR2(1000),
		stag_owner VARCHAR2(100),
		stag_ts_stage_data VARCHAR2(100),
		stag_ts_stage_indx VARCHAR2(100),
		stag_ts_hist_data VARCHAR2(100),
		stag_ts_hist_indx VARCHAR2(100),
		stag_fb_archive VARCHAR2(100),
		stag_bodi_ds VARCHAR2(100),
		stag_source_bodi_ds VARCHAR2(100)'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;