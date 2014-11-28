BEGIN
   p#frm#ddls.prc_create_entity (
      'p#frm#'
    , 'mesr_exec'
    , 'mesr_keyfigure_id NUMBER,
	   mesr_exec_result_value NUMBER,
	   mesr_exec_result_report CLOB'
    , 'DROP'
    , TRUE
    , TRUE
   );
END;