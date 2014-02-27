SET serveroutput ON;

BEGIN
   utl_ddl.prc_create_object_standard ('etl_stage_curr_hist'
                                         , 'etl_stage_curr_object_id NUMBER,
			                                etl_stage_hist_object_id NUMBER'
                                         , 'DROP'
                                         , TRUE
                                         , TRUE
                                          );
END;
/

ALTER TABLE etl_stage_curr_hist_t ADD (CONSTRAINT etl_stage_curr_hist_un UNIQUE (etl_stage_curr_object_id,etl_stage_hist_object_id));

COMMENT ON TABLE etl_stage_curr_hist_t IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';